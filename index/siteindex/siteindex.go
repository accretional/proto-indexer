// Package siteindex builds index.sqlite from a directory of per-repo sqlite files.
// The schema matches what the query-overlay client expects: a `databases` table
// describing each available DB file and a `database_metadata` table for snapshots.
package siteindex

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/accretional/proto-indexer/schema"
)

// dbKind is the variant of a per-repo sqlite file.
type dbKind int

const (
	kindSource dbKind = iota
	kindPackages
	kindSymbols
)

type fileEntry struct {
	name     string // human-readable, e.g. "myrepo — source"
	path     string // db_path as served, e.g. "/myrepo.source.sqlite"
	kind     dbKind
	ddl      string
	filePath string // on-disk absolute path
}

// Options controls index.sqlite generation.
type Options struct {
	// OutDir is the directory containing per-repo .sqlite files.
	OutDir string
	// IndexOut is where index.sqlite will be written (defaults to OutDir/index.sqlite).
	IndexOut string
	// BasePath is prepended to filenames to form db_path (e.g. "/out/").
	BasePath string
	// ProtoOnly skips repos that don't have all three variants (source+packages+symbols).
	ProtoOnly bool
}

// Build scans OutDir for per-repo sqlite files and writes index.sqlite.
func Build(ctx context.Context, opts Options) error {
	if opts.IndexOut == "" {
		opts.IndexOut = filepath.Join(opts.OutDir, "index.sqlite")
	}
	if opts.BasePath == "" {
		opts.BasePath = "/"
	}
	if !strings.HasSuffix(opts.BasePath, "/") {
		opts.BasePath += "/"
	}

	entries, err := collectEntries(opts)
	if err != nil {
		return err
	}

	db, err := schema.OpenDB(ctx, opts.IndexOut, schema.SiteIndexDDL)
	if err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, e := range entries {
		fileSize, numRecords, err := statDB(e.filePath, e.kind)
		if err != nil {
			return fmt.Errorf("stat %s: %w", e.filePath, err)
		}

		resolver := urlResolver(e.kind)

		_, err = tx.ExecContext(ctx,
			`INSERT INTO databases (db_name, db_path, db_schema, url_resolver, num_records, file_size, index_time)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			e.name, e.path, e.ddl, resolver, numRecords, fileSize, now,
		)
		if err != nil {
			return fmt.Errorf("insert databases %s: %w", e.name, err)
		}

		id := deterministicUUID(e.path, now)
		_, err = tx.ExecContext(ctx,
			`INSERT INTO database_metadata (uuid, db_path, num_records, file_size, index_time)
			 VALUES (?, ?, ?, ?, ?)`,
			id, e.path, numRecords, fileSize, now,
		)
		if err != nil {
			return fmt.Errorf("insert database_metadata %s: %w", e.name, err)
		}
	}

	return tx.Commit()
}

func collectEntries(opts Options) ([]fileEntry, error) {
	sourceFiles, err := filepath.Glob(filepath.Join(opts.OutDir, "*.source.sqlite"))
	if err != nil {
		return nil, err
	}

	// Build a set of repos that have all three variants if ProtoOnly is set.
	protoRepos := map[string]bool{}
	if opts.ProtoOnly {
		pkgFiles, _ := filepath.Glob(filepath.Join(opts.OutDir, "*.packages.sqlite"))
		symFiles, _ := filepath.Glob(filepath.Join(opts.OutDir, "*.symbols.sqlite"))
		hasPackages := fileSet(pkgFiles, ".packages.sqlite")
		hasSymbols := fileSet(symFiles, ".symbols.sqlite")
		for _, f := range sourceFiles {
			repo := repoName(f, ".source.sqlite")
			if hasPackages[repo] && hasSymbols[repo] {
				protoRepos[repo] = true
			}
		}
	}

	var entries []fileEntry

	for _, f := range sourceFiles {
		repo := repoName(f, ".source.sqlite")
		if opts.ProtoOnly && !protoRepos[repo] {
			continue
		}
		entries = append(entries, fileEntry{
			name:     repo + " — source",
			path:     opts.BasePath + filepath.Base(f),
			kind:     kindSource,
			ddl:      schema.SourceDDL,
			filePath: f,
		})
	}

	pkgFiles, _ := filepath.Glob(filepath.Join(opts.OutDir, "*.packages.sqlite"))
	for _, f := range pkgFiles {
		repo := repoName(f, ".packages.sqlite")
		if opts.ProtoOnly && !protoRepos[repo] {
			continue
		}
		entries = append(entries, fileEntry{
			name:     repo + " — packages",
			path:     opts.BasePath + filepath.Base(f),
			kind:     kindPackages,
			ddl:      schema.PackagesDDL,
			filePath: f,
		})
	}

	symFiles, _ := filepath.Glob(filepath.Join(opts.OutDir, "*.symbols.sqlite"))
	for _, f := range symFiles {
		repo := repoName(f, ".symbols.sqlite")
		if opts.ProtoOnly && !protoRepos[repo] {
			continue
		}
		entries = append(entries, fileEntry{
			name:     repo + " — symbols",
			path:     opts.BasePath + filepath.Base(f),
			kind:     kindSymbols,
			ddl:      schema.SymbolsDDL,
			filePath: f,
		})
	}

	return entries, nil
}

func statDB(path string, kind dbKind) (fileSize int64, numRecords int64, err error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	fileSize = fi.Size()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fileSize, 0, err
	}
	defer db.Close()

	table := mainTable(kind)
	row := db.QueryRow("SELECT COUNT(*) FROM " + table)
	if err := row.Scan(&numRecords); err != nil {
		return fileSize, 0, err
	}
	return fileSize, numRecords, nil
}

func mainTable(kind dbKind) string {
	switch kind {
	case kindPackages:
		return "packages"
	case kindSymbols:
		return "symbols"
	default:
		return "files"
	}
}

func urlResolver(kind dbKind) *string {
	var s string
	switch kind {
	case kindSource:
		// Bind :id (no underscore) and read repo_url/path from the DB directly,
		// avoiding JS parameter parsers that stop at '_' in named params.
		// Strip .git suffix from clone URL so the GitHub blob URL is valid.
		s = `SELECT CASE WHEN repo_url LIKE '%.git' THEN SUBSTR(repo_url, 1, LENGTH(repo_url)-4) ELSE repo_url END || '/blob/HEAD/' || path FROM files WHERE id = :id`
	case kindSymbols:
		// Bind :fqn; repo_url and file_path are read from the DB.
		s = `SELECT CASE WHEN repo_url LIKE '%.git' THEN SUBSTR(repo_url, 1, LENGTH(repo_url)-4) ELSE repo_url END || '/blob/HEAD/' || file_path FROM symbols WHERE fqn = :fqn`
	default:
		return nil
	}
	return &s
}

func repoName(path, suffix string) string {
	return strings.TrimSuffix(filepath.Base(path), suffix)
}

func fileSet(paths []string, suffix string) map[string]bool {
	m := make(map[string]bool, len(paths))
	for _, p := range paths {
		m[repoName(p, suffix)] = true
	}
	return m
}

// deterministicUUID produces a stable UUID from db_path + index_time so that
// re-running at the same timestamp yields the same rows (idempotent deploys).
func deterministicUUID(dbPath, indexTime string) string {
	h := sha256.Sum256([]byte(dbPath + "\x00" + indexTime))
	// Overwrite version (4) and variant bits to form a valid UUID v4 shape.
	h[6] = (h[6] & 0x0f) | 0x40
	h[8] = (h[8] & 0x3f) | 0x80
	return uuid.UUID(h[:16]).String()
}
