package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/accretional/proto-indexer/schema"
	"github.com/accretional/proto-repo/scan"
)

// TestE2ELocalRepo exercises the full processRepo pipeline against a
// locally-constructed git repo (served via file:// URL), avoiding any
// network dependency.
func TestE2ELocalRepo(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}

	// Seed a minimal repo with one .proto file and one Go file.
	proto := `syntax = "proto3";
package demo.v1;
message Hello { string name = 1; }
service Greeter { rpc SayHi(Hello) returns (Hello); }
`
	gofile := `package demo

// Greeter is a demo.
func Greeter() string { return "hi" }
`
	if err := os.WriteFile(filepath.Join(src, "demo.proto"), []byte(proto), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "demo.go"), []byte(gofile), 0o644); err != nil {
		t.Fatal(err)
	}

	// git init + commit
	for _, args := range [][]string{
		{"init", "-q", "-b", "main"},
		{"-c", "user.email=t@t", "-c", "user.name=t", "add", "."},
		{"-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "seed"},
	} {
		c := exec.Command("git", args...)
		c.Dir = src
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}

	out := filepath.Join(tmp, "out")
	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	repo := scan.Repo{
		Owner:         "local",
		Name:          "demo",
		FullName:      "local/demo",
		CloneURL:      "file://" + src,
		DefaultBranch: "main",
	}
	res, err := processRepo(context.Background(), repo, scratch, out, true, nil)
	if err != nil {
		t.Fatalf("processRepo: %v", err)
	}
	if res != resOK {
		t.Fatalf("expected resOK, got %v", res)
	}

	ctx := context.Background()

	// Validate source DB
	srcDB := filepath.Join(out, "demo.source.sqlite")
	if _, err := os.Stat(srcDB); err != nil {
		t.Fatalf("source db missing: %v", err)
	}
	row, err := schema.Attach(srcDB).QueryOne(ctx, `SELECT COUNT(*) FROM files`)
	if err != nil {
		t.Fatal(err)
	}
	srcFiles, err := schema.CellInt(row, 0)
	if err != nil {
		t.Fatal(err)
	}
	if srcFiles < 2 {
		t.Errorf("expected >=2 source files, got %d", srcFiles)
	}

	// Validate packages DB
	pkgDBPath := filepath.Join(out, "demo.packages.sqlite")
	row, err = schema.Attach(pkgDBPath).QueryOne(ctx, `SELECT proto_package FROM packages LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	if pkg := schema.CellText(row, 0); pkg != "demo.v1" {
		t.Errorf("expected package demo.v1, got %q", pkg)
	}

	// Validate symbols DB (separate file, correlated by proto_package text)
	symDBPath := filepath.Join(out, "demo.symbols.sqlite")
	row, err = schema.Attach(symDBPath).QueryOne(ctx,
		`SELECT fqn, input_fqn, output_fqn, proto_package, repo_url FROM symbols WHERE kind='method'`)
	if err != nil {
		t.Fatal(err)
	}
	method := schema.CellText(row, 0)
	in := schema.CellText(row, 1)
	outFQN := schema.CellText(row, 2)
	symPkg := schema.CellText(row, 3)
	symURL := schema.CellText(row, 4)
	if method != "demo.v1.Greeter.SayHi" {
		t.Errorf("method FQN = %q, want demo.v1.Greeter.SayHi", method)
	}
	if in != "demo.v1.Hello" || outFQN != "demo.v1.Hello" {
		t.Errorf("method io = %q -> %q, want demo.v1.Hello -> demo.v1.Hello", in, outFQN)
	}
	if symPkg != "demo.v1" {
		t.Errorf("symbol proto_package = %q, want demo.v1", symPkg)
	}
	if symURL != repo.CloneURL {
		t.Errorf("symbol repo_url = %q, want %q", symURL, repo.CloneURL)
	}
}
