// Package source indexes a repository's source files into a SQLite DB.
package source

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/accretional/proto-indexer/index/embed"
	"github.com/accretional/proto-indexer/schema"
	sqlitepb "github.com/accretional/proto-sqlite/sqlite/pb"
)

// skipDirs are directory names we never descend into.
var skipDirs = map[string]bool{
	".git":         true,
	"vendor":       true,
	"node_modules": true,
	"third_party":  true,
	"build":        true,
	"dist":         true,
	".venv":        true,
	"__pycache__":  true,
}

// maxFileSize is the upper bound for files we'll store full content for (1 MiB).
// Larger files still get a row, but content is empty.
const maxFileSize = 1 << 20

// maxChunkBytes caps the estimated post-substitution size of one batched
// BEGIN/INSERT/COMMIT payload. The proto-sqlite server passes the entire
// SQL string as a single argv to sqlite3, so this must stay well under
// ARG_MAX (~1 MiB on Darwin).
const maxChunkBytes = 500 * 1024

type srcRow struct {
	path, language, sha256, content string
	size                            int64
}

// estimateBytes approximates the row's contribution to post-substitution
// SQL text. text = 2 + len (ignoring the rare case of many '' doublings);
// int, short strings round to a small constant.
func (r *srcRow) estimateBytes() int {
	return 32 + len(r.path) + len(r.language) + len(r.sha256) + len(r.content)
}

// Index walks repoPath and writes every eligible source file into outPath as a
// fresh SQLite DB. repoLabel (owner/name) and repoURL (clone/origin URL) are
// stored on every row. provider is optional; when non-nil, a vector is computed
// for each file and stored in files_vectors. Files with empty content are skipped.
func Index(ctx context.Context, repoPath, repoLabel, repoURL, outPath string, provider embed.Provider) error {
	db, err := schema.OpenDB(ctx, outPath, schema.SourceDDL)
	if err != nil {
		return err
	}

	var (
		batch      []srcRow
		batchBytes int
		flushErr   error
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := flushSourceBatch(ctx, db, repoLabel, repoURL, batch); err != nil {
			return err
		}
		if provider != nil {
			if err := embedBatch(ctx, db, repoLabel, batch, provider); err != nil {
				return err
			}
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}

	walkErr := filepath.WalkDir(repoPath, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return nil
		}
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil || !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(repoPath, path)
		if err != nil {
			rel = path
		}

		var content []byte
		if info.Size() <= maxFileSize {
			if b, err := os.ReadFile(path); err == nil && utf8.Valid(b) {
				content = b
			}
		}
		sum := sha256.Sum256(content)
		row := srcRow{
			path:     rel,
			language: language(rel),
			sha256:   hex.EncodeToString(sum[:]),
			content:  string(content),
			size:     info.Size(),
		}
		rowBytes := row.estimateBytes()
		if batchBytes+rowBytes > maxChunkBytes {
			if flushErr = flush(); flushErr != nil {
				return filepath.SkipAll
			}
		}
		batch = append(batch, row)
		batchBytes += rowBytes
		return nil
	})
	if walkErr != nil {
		return walkErr
	}
	if flushErr != nil {
		return flushErr
	}
	return flush()
}

func flushSourceBatch(ctx context.Context, db *schema.DB, repoLabel, repoURL string, rows []srcRow) error {
	var sb strings.Builder
	sb.WriteString("BEGIN;\nINSERT INTO files(repo, repo_url, path, language, size, sha256, content) VALUES ")
	params := make([]*sqlitepb.Value, 0, 7*len(rows))
	for i, r := range rows {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?,?,?,?)")
		params = append(params,
			schema.Text(repoLabel),
			schema.Text(repoURL),
			schema.Text(r.path),
			schema.Text(r.language),
			schema.Int(r.size),
			schema.Text(r.sha256),
			schema.Text(r.content),
		)
	}
	sb.WriteString(";\nCOMMIT;")
	if err := db.Exec(ctx, sb.String(), params...); err != nil {
		return fmt.Errorf("source: flush %d rows: %w", len(rows), err)
	}
	return nil
}

// embedBatch fetches file IDs for the just-flushed batch, calls the provider,
// and writes vectors into files_vectors. Files with empty content are skipped.
// Provider errors per-file are logged but do not abort the batch.
func embedBatch(ctx context.Context, db *schema.DB, repoLabel string, rows []srcRow, provider embed.Provider) error {
	// Build parallel slices of paths and content, skipping empty-content files.
	type entry struct {
		path    string
		content string
	}
	var eligible []entry
	for _, r := range rows {
		if r.content != "" {
			eligible = append(eligible, entry{r.path, r.content})
		}
	}
	if len(eligible) == 0 {
		return nil
	}

	texts := make([]string, len(eligible))
	for i, e := range eligible {
		texts[i] = e.content
	}

	vecs, err := provider.Embed(ctx, texts)
	if err != nil {
		return fmt.Errorf("source: embed batch: %w", err)
	}

	// Look up file IDs by (repo, path).
	var toFlush []vecRow
	for i, e := range eligible {
		if vecs[i] == nil {
			continue
		}
		resp, err := db.QueryOne(ctx,
			`SELECT id FROM files WHERE repo=? AND path=?`,
			schema.Text(repoLabel), schema.Text(e.path),
		)
		if err != nil {
			// Soft failure: log and skip.
			fmt.Printf("source: lookup id for %s: %v\n", e.path, err)
			continue
		}
		id, err := schema.CellInt(resp, 0)
		if err != nil {
			continue
		}
		toFlush = append(toFlush, vecRow{id, vecs[i]})
	}

	if len(toFlush) == 0 {
		return nil
	}
	return flushVectorBatch(ctx, db, toFlush, provider.Name(), provider.Model())
}

type vecRow struct {
	fileID int64
	vec    []float32
}

func flushVectorBatch(ctx context.Context, db *schema.DB, rows []vecRow, providerName, modelName string) error {
	var sb strings.Builder
	sb.WriteString("BEGIN;\nINSERT INTO files_vectors(file_id, provider, model, vector) VALUES ")
	params := make([]*sqlitepb.Value, 0, 4*len(rows))
	for i, r := range rows {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?)")
		params = append(params,
			schema.Int(r.fileID),
			schema.Text(providerName),
			schema.Text(modelName),
			schema.Blob(float32sToBytes(r.vec)),
		)
	}
	sb.WriteString(";\nCOMMIT;")
	if err := db.Exec(ctx, sb.String(), params...); err != nil {
		return fmt.Errorf("source: flush %d vectors: %w", len(rows), err)
	}
	return nil
}

func float32sToBytes(v []float32) []byte {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

func language(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".go":
		return "go"
	case ".proto":
		return "proto"
	case ".py":
		return "python"
	case ".js", ".mjs", ".cjs":
		return "javascript"
	case ".ts", ".tsx":
		return "typescript"
	case ".rs":
		return "rust"
	case ".java":
		return "java"
	case ".kt":
		return "kotlin"
	case ".c", ".h":
		return "c"
	case ".cc", ".cpp", ".hpp", ".hh":
		return "cpp"
	case ".rb":
		return "ruby"
	case ".sh", ".bash":
		return "shell"
	case ".md":
		return "markdown"
	case ".yaml", ".yml":
		return "yaml"
	case ".json":
		return "json"
	case ".toml":
		return "toml"
	case ".sql":
		return "sql"
	}
	return ""
}
