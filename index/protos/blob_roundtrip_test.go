package protos

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/accretional/proto-indexer/schema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TestBlobRoundTrip pins that BLOB bytes written through proto-sqlite
// (which hex-encodes via X'...' in .mode quote) come back identical
// after parseQuote decodes them. Regression guard for the Issue #2 fix.
func TestBlobRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "blob.sqlite")
	db, err := schema.OpenDB(ctx, path, `CREATE TABLE b (id INTEGER PRIMARY KEY, data BLOB NOT NULL);`)
	if err != nil {
		t.Fatal(err)
	}

	want := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("demo.proto"),
		Package: proto.String("demo.v1"),
	}
	raw, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Exec(ctx, `INSERT INTO b(data) VALUES (?)`, schema.Blob(raw)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	row, err := db.QueryOne(ctx, `SELECT data FROM b LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	got := schema.CellBlob(row, 0)

	var back descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(got, &back); err != nil {
		t.Fatalf("unmarshal round-tripped blob: %v", err)
	}
	if back.GetName() != "demo.proto" || back.GetPackage() != "demo.v1" {
		t.Errorf("round-trip mismatch: got name=%q package=%q", back.GetName(), back.GetPackage())
	}
}
