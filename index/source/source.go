// Package source indexes a repository's source files into a SQLite DB.
package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

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
// stored on every row.
func Index(ctx context.Context, repoPath, repoLabel, repoURL, outPath string) error {
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
		err := flushSourceBatch(ctx, db, repoLabel, repoURL, batch)
		batch = batch[:0]
		batchBytes = 0
		return err
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
