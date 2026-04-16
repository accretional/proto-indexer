package source

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/accretional/proto-indexer/schema"
)

func TestIndexSelf(t *testing.T) {
	// Index this repo (three dirs up from source_test.go: index/source/ -> repo root).
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "self.source.sqlite")
	ctx := context.Background()
	if err := Index(ctx, repoRoot, "proto-repo", "file://"+repoRoot, out); err != nil {
		t.Fatalf("Index: %v", err)
	}

	db := schema.Attach(out)
	row, err := db.QueryOne(ctx, `SELECT COUNT(*) FROM files`)
	if err != nil {
		t.Fatal(err)
	}
	n, err := schema.CellInt(row, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("expected files rows, got 0")
	}

	row, err = db.QueryOne(ctx, `SELECT COUNT(*) FROM files_fts WHERE files_fts MATCH 'gitfetch'`)
	if err != nil {
		t.Fatal(err)
	}
	hits, err := schema.CellInt(row, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hits == 0 {
		t.Fatal("expected FTS hits for 'gitfetch', got 0")
	}
	t.Logf("indexed %d files, %d FTS matches for 'gitfetch'", n, hits)
}
