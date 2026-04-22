package protos

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/accretional/proto-indexer/schema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TestBlobRoundTrip verifies that proto BLOB bytes survive a write/read cycle
// through the SQLite driver and schema.CellBlob decoder byte-identical.
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

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO b(data) VALUES (?)`, raw); err != nil {
		tx.Rollback()
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
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
