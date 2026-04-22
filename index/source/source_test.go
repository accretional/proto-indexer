package source

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/accretional/proto-indexer/index/embed"
	"github.com/accretional/proto-indexer/schema"
)

// stubProvider returns a fixed-dimension vector of ones for every input.
type stubProvider struct{}

func (stubProvider) Name() string  { return "stub" }
func (stubProvider) Model() string { return "ones" }
func (stubProvider) Dimension() int { return 4 }
func (stubProvider) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		if t != "" {
			out[i] = []float32{1, 0, 0, 0}
		}
	}
	return out, nil
}

var _ embed.Provider = stubProvider{}

func TestIndexSelf(t *testing.T) {
	// Index this repo (three dirs up from source_test.go: index/source/ -> repo root).
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "self.source.sqlite")
	ctx := context.Background()
	if err := Index(ctx, repoRoot, "proto-indexer", "file://"+repoRoot, out, nil); err != nil {
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

	row, err = db.QueryOne(ctx, `SELECT COUNT(*) FROM files_fts WHERE files_fts MATCH 'protocompile'`)
	if err != nil {
		t.Fatal(err)
	}
	hits, err := schema.CellInt(row, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hits == 0 {
		t.Fatal("expected FTS hits for 'protocompile', got 0")
	}
	t.Logf("indexed %d files, %d FTS matches for 'protocompile'", n, hits)
}

func TestIndexWithEmbedProvider(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "embed.source.sqlite")
	ctx := context.Background()
	if err := Index(ctx, repoRoot, "proto-repo", "file://"+repoRoot, out, stubProvider{}); err != nil {
		t.Fatalf("Index: %v", err)
	}

	db := schema.Attach(out)

	row, err := db.QueryOne(ctx, `SELECT COUNT(*) FROM files_vectors`)
	if err != nil {
		t.Fatal(err)
	}
	nVecs, err := schema.CellInt(row, 0)
	if err != nil {
		t.Fatal(err)
	}
	if nVecs == 0 {
		t.Fatal("expected files_vectors rows, got 0")
	}

	// Spot-check: every vector row references a valid file and has the right provider/model.
	row, err = db.QueryOne(ctx, `
		SELECT v.provider, v.model, length(v.vector)
		FROM files_vectors v
		JOIN files f ON f.id = v.file_id
		LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	if schema.CellText(row, 0) != "stub" {
		t.Errorf("provider = %q, want stub", schema.CellText(row, 0))
	}
	if schema.CellText(row, 1) != "ones" {
		t.Errorf("model = %q, want ones", schema.CellText(row, 1))
	}
	blobLen, _ := schema.CellInt(row, 2)
	if blobLen != 16 { // 4 float32 * 4 bytes
		t.Errorf("vector blob length = %d, want 16", blobLen)
	}
	t.Logf("files_vectors: %d rows", nVecs)
}
