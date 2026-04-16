// Command indexer fetches repos from a GitHub org and produces per-repo
// SQLite indexes: <repo>.source.sqlite (source files + FTS5),
// <repo>.packages.sqlite (proto packages + FileDescriptorSet blobs), and
// <repo>.symbols.sqlite (proto symbols).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/accretional/proto-indexer/index/protos"
	"github.com/accretional/proto-indexer/index/source"
	"github.com/accretional/proto-indexer/protocompile"
	"github.com/accretional/proto-repo/gitfetch"
	"github.com/accretional/proto-repo/scan"
)

func main() {
	var (
		org        = flag.String("org", "", "GitHub org or user to scan (mutually exclusive with --repo)")
		repoFlag   = flag.String("repo", "", "single GitHub repo as owner/name (mutually exclusive with --org)")
		outDir     = flag.String("out-dir", "./out", "directory to write per-repo .sqlite files")
		scratchDir = flag.String("scratch-dir", "./scratch", "directory to clone repos into")
		token      = flag.String("token", "", "GitHub token (falls back to GITHUB_TOKEN env, then gh CLI)")
		workers    = flag.Int("workers", 4, "parallel repos to process")
		shallow    = flag.Bool("shallow", true, "use shallow clone")
		timeout    = flag.Duration("timeout", 10*time.Minute, "per-repo timeout")
	)
	flag.Parse()
	if (*org == "") == (*repoFlag == "") {
		fmt.Fprintln(os.Stderr, "exactly one of --org or --repo is required")
		flag.Usage()
		os.Exit(2)
	}

	tok := *token
	if tok == "" {
		tok = os.Getenv("GITHUB_TOKEN")
	}
	if tok == "" {
		tok = scan.TokenFromGHCLI()
	}

	for _, d := range []string{*outDir, *scratchDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			log.Fatalf("mkdir %s: %v", d, err)
		}
	}

	ctx := context.Background()
	gh := scan.NewGithubClient(tok)

	var repos []scan.Repo
	if *repoFlag != "" {
		r, err := gh.GetRepo(ctx, *repoFlag)
		if err != nil {
			log.Fatalf("get repo: %v", err)
		}
		repos = []scan.Repo{r}
		log.Printf("targeting single repo %s", r.FullName)
	} else {
		var err error
		repos, err = gh.ListRepos(ctx, *org)
		if err != nil {
			log.Fatalf("list repos: %v", err)
		}
		log.Printf("found %d repos in %s", len(repos), *org)
	}

	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	stats := struct{ ok, fail, noproto int }{}

	for _, r := range repos {
		if r.DefaultBranch == "" || r.CloneURL == "" {
			continue
		}
		r := r
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			rctx, cancel := context.WithTimeout(ctx, *timeout)
			defer cancel()

			result, err := processRepo(rctx, r, *scratchDir, *outDir, *shallow)
			mu.Lock()
			defer mu.Unlock()
			switch result {
			case resOK:
				stats.ok++
				log.Printf("[ok]      %s", r.FullName)
			case resNoProto:
				stats.noproto++
				log.Printf("[source]  %s (no .proto files)", r.FullName)
			case resFail:
				stats.fail++
				log.Printf("[fail]    %s: %v", r.FullName, err)
			}
		}()
	}
	wg.Wait()

	log.Printf("done: %d ok, %d source-only, %d failed", stats.ok, stats.noproto, stats.fail)
	fmt.Printf("indexes written to %s\n", *outDir)
}

type result int

const (
	resOK result = iota
	resNoProto
	resFail
)

func processRepo(ctx context.Context, r scan.Repo, scratchDir, outDir string, shallow bool) (result, error) {
	fetched, err := gitfetch.Fetch(ctx, r.CloneURL, scratchDir, r.Name, shallow)
	if err != nil {
		return resFail, fmt.Errorf("fetch: %w", err)
	}

	srcOut := filepath.Join(outDir, r.Name+".source.sqlite")
	if err := source.Index(ctx, fetched.Path, r.FullName, r.CloneURL, srcOut); err != nil {
		return resFail, fmt.Errorf("source index: %w", err)
	}

	fds, err := protocompile.Compile(ctx, fetched.Path)
	if err != nil {
		// Surface compile failures but don't nuke the whole repo's output —
		// we already have source.sqlite. Caller logs the error.
		return resFail, fmt.Errorf("protoc: %w", err)
	}
	if fds == nil {
		return resNoProto, nil
	}

	pkgOut := filepath.Join(outDir, r.Name+".packages.sqlite")
	symOut := filepath.Join(outDir, r.Name+".symbols.sqlite")
	if err := protos.Index(ctx, fds, r.FullName, r.CloneURL, pkgOut, symOut); err != nil {
		return resFail, fmt.Errorf("protos index: %w", err)
	}
	return resOK, nil
}
