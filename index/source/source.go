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

// batchSize is the number of rows to accumulate before flushing to SQLite.
const batchSize = 500

type srcRow struct {
	path, language, sha256, content string
	size                            int64
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
		batch    []srcRow
		flushErr error
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
		batch = append(batch, srcRow{
			path:     rel,
			language: language(rel),
			sha256:   hex.EncodeToString(sum[:]),
			content:  string(content),
			size:     info.Size(),
		})
		if len(batch) >= batchSize {
			if flushErr = flush(); flushErr != nil {
				return filepath.SkipAll
			}
		}
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
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("source: begin tx: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO files(repo, repo_url, path, language, size, sha256, content) VALUES (?,?,?,?,?,?,?)`)
	if err != nil {
		return fmt.Errorf("source: prepare insert: %w", err)
	}
	defer stmt.Close()
	for _, r := range rows {
		if _, err := stmt.ExecContext(ctx, repoLabel, repoURL, r.path, r.language, r.size, r.sha256, r.content); err != nil {
			return fmt.Errorf("source: insert %s: %w", r.path, err)
		}
	}
	return tx.Commit()
}

// embedBatch fetches file IDs for the just-flushed batch, calls the provider,
// and writes vectors into files_vectors. Files with empty content are skipped.
// Provider errors per-file are logged but do not abort the batch.
func embedBatch(ctx context.Context, db *schema.DB, repoLabel string, rows []srcRow, provider embed.Provider) error {
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

	var toFlush []vecRow
	for i, e := range eligible {
		if vecs[i] == nil {
			continue
		}
		row, err := db.QueryOne(ctx, `SELECT id FROM files WHERE repo=? AND path=?`, repoLabel, e.path)
		if err != nil {
			fmt.Printf("source: lookup id for %s: %v\n", e.path, err)
			continue
		}
		id, err := schema.CellInt(row, 0)
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
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("source: begin vector tx: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO files_vectors(file_id, provider, model, vector) VALUES (?,?,?,?)`)
	if err != nil {
		return fmt.Errorf("source: prepare vector insert: %w", err)
	}
	defer stmt.Close()
	for _, r := range rows {
		if _, err := stmt.ExecContext(ctx, r.fileID, providerName, modelName, float32sToBytes(r.vec)); err != nil {
			return fmt.Errorf("source: insert vector for file %d: %w", r.fileID, err)
		}
	}
	return tx.Commit()
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
