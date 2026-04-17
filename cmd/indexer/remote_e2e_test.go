package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/accretional/proto-indexer/schema"
	"github.com/accretional/proto-repo/scan"
)

// TestE2ERemoteRepo exercises the full fetch → index pipeline against a real
// GitHub repo. Opt-in: requires a GitHub token via PROTO_INDEXER_E2E_TOKEN
// or GITHUB_TOKEN. Skipped otherwise, including under -short.
//
// The target repo defaults to accretional/proto-merge (small, public, ships
// protos) and can be overridden via PROTO_INDEXER_E2E_REPO=owner/name.
func TestE2ERemoteRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping remote e2e in -short mode")
	}
	tok := firstNonEmpty(
		os.Getenv("PROTO_INDEXER_E2E_TOKEN"),
		os.Getenv("GITHUB_TOKEN"),
	)
	if tok == "" {
		tok = scan.TokenFromGHCLI()
	}
	if tok == "" {
		t.Skip("no GitHub token (set PROTO_INDEXER_E2E_TOKEN or GITHUB_TOKEN)")
	}

	fullName := os.Getenv("PROTO_INDEXER_E2E_REPO")
	if fullName == "" {
		fullName = "accretional/proto-merge"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	gh := scan.NewGithubClient(tok)
	repo, err := gh.GetRepo(ctx, fullName)
	if err != nil {
		t.Fatalf("GetRepo %s: %v", fullName, err)
	}
	if repo.CloneURL == "" || repo.DefaultBranch == "" {
		t.Fatalf("repo metadata incomplete: %+v", repo)
	}

	tmp := t.TempDir()
	out := filepath.Join(tmp, "out")
	scratch := filepath.Join(tmp, "scratch")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}

	res, err := processRepo(ctx, repo, scratch, out, true, nil)
	if err != nil {
		t.Fatalf("processRepo: %v", err)
	}
	if res != resOK {
		t.Fatalf("expected resOK, got %v", res)
	}

	srcDB := filepath.Join(out, repo.Name+".source.sqlite")
	pkgDBPath := filepath.Join(out, repo.Name+".packages.sqlite")
	symDBPath := filepath.Join(out, repo.Name+".symbols.sqlite")
	for _, p := range []string{srcDB, pkgDBPath, symDBPath} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("output missing: %s: %v", p, err)
		}
	}

	srcFiles := countRows(t, ctx, srcDB, `SELECT COUNT(*) FROM files`)
	if srcFiles < 1 {
		t.Errorf("source files = %d, want >=1", srcFiles)
	}
	assertURL(t, ctx, srcDB, `SELECT repo_url FROM files LIMIT 1`, repo.CloneURL)

	pkgRows := countRows(t, ctx, pkgDBPath, `SELECT COUNT(*) FROM packages`)
	if pkgRows < 1 {
		t.Errorf("packages rows = %d, want >=1", pkgRows)
	}
	assertURL(t, ctx, pkgDBPath, `SELECT repo_url FROM packages LIMIT 1`, repo.CloneURL)

	symRows := countRows(t, ctx, symDBPath, `SELECT COUNT(*) FROM symbols`)
	if symRows < 1 {
		t.Errorf("symbols rows = %d, want >=1", symRows)
	}
	assertURL(t, ctx, symDBPath, `SELECT repo_url FROM symbols LIMIT 1`, repo.CloneURL)

	t.Logf("%s: %d files, %d packages, %d symbols", fullName, srcFiles, pkgRows, symRows)
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

func countRows(t *testing.T, ctx context.Context, dbPath, query string) int {
	t.Helper()
	row, err := schema.Attach(dbPath).QueryOne(ctx, query)
	if err != nil {
		t.Fatalf("%s on %s: %v", query, dbPath, err)
	}
	n, err := schema.CellInt(row, 0)
	if err != nil {
		t.Fatalf("decode count from %s: %v", dbPath, err)
	}
	return int(n)
}

func assertURL(t *testing.T, ctx context.Context, dbPath, query, want string) {
	t.Helper()
	row, err := schema.Attach(dbPath).QueryOne(ctx, query)
	if err != nil {
		t.Fatalf("%s on %s: %v", query, dbPath, err)
	}
	if got := schema.CellText(row, 0); got != want {
		t.Errorf("%s: repo_url = %q, want %q", dbPath, got, want)
	}
}
