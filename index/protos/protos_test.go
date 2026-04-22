package protos

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/accretional/proto-indexer/schema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// hand-built FDS: one file in demo.v1 with a message, an enum, a service, and a method.
func testFDS() *descriptorpb.FileDescriptorSet {
	return &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("demo.proto"),
				Package: proto.String("demo.v1"),
				MessageType: []*descriptorpb.DescriptorProto{
					{Name: proto.String("Hello")},
				},
				EnumType: []*descriptorpb.EnumDescriptorProto{
					{
						Name: proto.String("Status"),
						Value: []*descriptorpb.EnumValueDescriptorProto{
							{Name: proto.String("UNKNOWN"), Number: proto.Int32(0)},
						},
					},
				},
				Service: []*descriptorpb.ServiceDescriptorProto{
					{
						Name: proto.String("Greeter"),
						Method: []*descriptorpb.MethodDescriptorProto{
							{
								Name:       proto.String("SayHi"),
								InputType:  proto.String(".demo.v1.Hello"),
								OutputType: proto.String(".demo.v1.Hello"),
							},
						},
					},
				},
				SourceCodeInfo: &descriptorpb.SourceCodeInfo{
					Location: []*descriptorpb.SourceCodeInfo_Location{
						// message Hello at line 3 (span[0] = 2 → line 3)
						{Path: []int32{int32(tagMessage), 0}, Span: []int32{2, 0, 10}},
						// enum Status at line 6
						{Path: []int32{int32(tagEnum), 0}, Span: []int32{5, 0, 10}},
						// service Greeter at line 9
						{Path: []int32{int32(tagService), 0}, Span: []int32{8, 0, 20}},
					},
				},
			},
		},
	}
}

func TestIndex(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	pkgPath := filepath.Join(tmp, "pkg.sqlite")
	symPath := filepath.Join(tmp, "sym.sqlite")

	if err := Index(ctx, testFDS(), "owner/repo", "https://github.com/owner/repo", pkgPath, symPath); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// --- packages DB ---
	pkgDB := schema.Attach(pkgPath)

	row, err := pkgDB.QueryOne(ctx, `SELECT proto_package, file_count FROM packages`)
	if err != nil {
		t.Fatalf("query packages: %v", err)
	}
	if got := schema.CellText(row, 0); got != "demo.v1" {
		t.Errorf("proto_package = %q, want demo.v1", got)
	}
	if got, _ := schema.CellInt(row, 1); got != 1 {
		t.Errorf("file_count = %d, want 1", got)
	}

	// descriptor_set blob should unmarshal back to a valid FDS containing demo.proto
	blobRow, err := pkgDB.QueryOne(ctx, `SELECT descriptor_set FROM packages`)
	if err != nil {
		t.Fatal(err)
	}
	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(schema.CellBlob(blobRow, 0), &fds); err != nil {
		t.Fatalf("unmarshal descriptor_set: %v", err)
	}
	if len(fds.File) != 1 || fds.File[0].GetName() != "demo.proto" {
		t.Errorf("descriptor_set files = %v, want [demo.proto]", fds.File)
	}

	// --- symbols DB ---
	symDB := schema.Attach(symPath)

	rows, err := symDB.Query(ctx, `SELECT kind, name, fqn, file_path, line FROM symbols ORDER BY kind, name`)
	if err != nil {
		t.Fatalf("query symbols: %v", err)
	}

	type sym struct{ kind, name, fqn string; line int64 }
	var got []sym
	for _, r := range rows {
		line, _ := schema.CellInt(r, 4)
		got = append(got, sym{schema.CellText(r, 0), schema.CellText(r, 1), schema.CellText(r, 2), line})
	}

	want := []sym{
		{"enum", "Status", "demo.v1.Status", 6},
		{"message", "Hello", "demo.v1.Hello", 3},
		{"method", "SayHi", "demo.v1.Greeter.SayHi", 0}, // method line not in SourceCodeInfo above
		{"service", "Greeter", "demo.v1.Greeter", 9},
	}
	if len(got) != len(want) {
		t.Fatalf("symbols count = %d, want %d; got %+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.kind != w.kind || g.name != w.name || g.fqn != w.fqn || g.line != w.line {
			t.Errorf("symbol[%d]: got {%s %s %s line=%d}, want {%s %s %s line=%d}",
				i, g.kind, g.name, g.fqn, g.line, w.kind, w.name, w.fqn, w.line)
		}
	}

	// method should have input_fqn / output_fqn set
	methodRow, err := symDB.QueryOne(ctx, `SELECT input_fqn, output_fqn FROM symbols WHERE kind='method'`)
	if err != nil {
		t.Fatalf("query method: %v", err)
	}
	if got := schema.CellText(methodRow, 0); got != "demo.v1.Hello" {
		t.Errorf("input_fqn = %q, want demo.v1.Hello", got)
	}
	if got := schema.CellText(methodRow, 1); got != "demo.v1.Hello" {
		t.Errorf("output_fqn = %q, want demo.v1.Hello", got)
	}

	// repo metadata stored on every symbol row
	repoRow, err := symDB.QueryOne(ctx, `SELECT repo, repo_url FROM symbols LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	if got := schema.CellText(repoRow, 0); got != "owner/repo" {
		t.Errorf("repo = %q, want owner/repo", got)
	}
	if got := schema.CellText(repoRow, 1); got != "https://github.com/owner/repo" {
		t.Errorf("repo_url = %q, want https://github.com/owner/repo", got)
	}
}

func TestIndex_EmptyFDS(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	pkgPath := filepath.Join(tmp, "pkg.sqlite")
	symPath := filepath.Join(tmp, "sym.sqlite")

	fds := &descriptorpb.FileDescriptorSet{}
	if err := Index(ctx, fds, "r", "u", pkgPath, symPath); err != nil {
		t.Fatalf("Index with empty FDS: %v", err)
	}

	row, err := schema.Attach(pkgPath).QueryOne(ctx, `SELECT COUNT(*) FROM packages`)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := schema.CellInt(row, 0); n != 0 {
		t.Errorf("packages count = %d, want 0", n)
	}
}
