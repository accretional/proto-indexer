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
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/accretional/proto-indexer/index/embed"
	"github.com/accretional/proto-indexer/index/protos"
	"github.com/accretional/proto-indexer/index/source"
	"github.com/accretional/proto-indexer/protocompile"
	"github.com/accretional/proto-repo/gitfetch"
	"github.com/accretional/proto-repo/scan"
)

func main() {
	var (
		org        = flag.String("org", "", "GitHub org or user to scan (mutually exclusive with --repo and --local)")
		repoFlag   = flag.String("repo", "", "single GitHub repo as owner/name (mutually exclusive with --org and --local)")
		localFlag  = flag.String("local", "", "path to a local repo to index (mutually exclusive with --org and --repo)")
		outDir     = flag.String("out-dir", "./out", "directory to write per-repo .sqlite files")
		scratchDir = flag.String("scratch-dir", "./scratch", "directory to clone repos into")
		token      = flag.String("token", "", "GitHub token (falls back to GITHUB_TOKEN env, then gh CLI)")
		workers           = flag.Int("workers", 4, "parallel repos to process")
		shallow           = flag.Bool("shallow", true, "use shallow clone")
		timeout           = flag.Duration("timeout", 10*time.Minute, "per-repo timeout")
		embeddingProvider = flag.String("embedding-provider", "", "embedding provider to use (apple)")
		embeddingBinary   = flag.String("embedding-binary", "", "path to provider binary (default: looked up on $PATH)")
	)
	flag.Parse()

	setCount := 0
	for _, s := range []string{*org, *repoFlag, *localFlag} {
		if s != "" {
			setCount++
		}
	}
	if setCount != 1 {
		fmt.Fprintln(os.Stderr, "exactly one of --org, --repo, or --local is required")
		flag.Usage()
		os.Exit(2)
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", *outDir, err)
	}

	var provider embed.Provider
	switch *embeddingProvider {
	case "", "none":
		// no embedding
	case "apple":
		provider = embed.NewApple(*embeddingBinary)
	default:
		log.Fatalf("unknown embedding provider: %s", *embeddingProvider)
	}

	ctx := context.Background()

	if *localFlag != "" {
		absPath, err := filepath.Abs(*localFlag)
		if err != nil {
			log.Fatalf("resolve local path: %v", err)
		}
		if _, err := os.Stat(absPath); err != nil {
			log.Fatalf("local path: %v", err)
		}
		log.Printf("indexing local repo at %s", absPath)
		res, err := indexLocal(ctx, absPath, *outDir, provider)
		switch res {
		case resOK:
			log.Printf("[ok]      %s", absPath)
		case resNoProto:
			log.Printf("[source]  %s (no .proto files)", absPath)
		case resFail:
			log.Fatalf("[fail]    %s: %v", absPath, err)
		}
		fmt.Printf("indexes written to %s\n", *outDir)
		return
	}

	tok := *token
	if tok == "" {
		tok = os.Getenv("GITHUB_TOKEN")
	}
	if tok == "" {
		tok = scan.TokenFromGHCLI()
	}

	if err := os.MkdirAll(*scratchDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", *scratchDir, err)
	}

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

			result, err := processRepo(rctx, r, *scratchDir, *outDir, *shallow, provider)
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

func processRepo(ctx context.Context, r scan.Repo, scratchDir, outDir string, shallow bool, provider embed.Provider) (result, error) {
	fetched, err := gitfetch.Fetch(ctx, r.CloneURL, scratchDir, r.Name, shallow)
	if err != nil {
		return resFail, fmt.Errorf("fetch: %w", err)
	}
	return indexPath(ctx, fetched.Path, r.Name, r.FullName, r.CloneURL, outDir, provider)
}

func indexLocal(ctx context.Context, absPath, outDir string, provider embed.Provider) (result, error) {
	name := filepath.Base(absPath)
	repoURL := gitOriginURL(absPath)
	if repoURL == "" {
		repoURL = "file://" + absPath
	}
	return indexPath(ctx, absPath, name, name, repoURL, outDir, provider)
}

func gitOriginURL(repoPath string) string {
	out, err := exec.Command("git", "-C", repoPath, "remote", "get-url", "origin").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func indexPath(ctx context.Context, repoPath, name, label, repoURL, outDir string, provider embed.Provider) (result, error) {
	srcOut := filepath.Join(outDir, name+".source.sqlite")
	if err := source.Index(ctx, repoPath, label, repoURL, srcOut, provider); err != nil {
		return resFail, fmt.Errorf("source index: %w", err)
	}

	fds, err := protocompile.Compile(ctx, repoPath)
	if err != nil {
		// Surface compile failures but don't nuke the whole repo's output —
		// we already have source.sqlite. Caller logs the error.
		return resFail, fmt.Errorf("protoc: %w", err)
	}
	if fds == nil {
		return resNoProto, nil
	}

	pkgOut := filepath.Join(outDir, name+".packages.sqlite")
	symOut := filepath.Join(outDir, name+".symbols.sqlite")
	if err := protos.Index(ctx, fds, label, repoURL, pkgOut, symOut); err != nil {
		return resFail, fmt.Errorf("protos index: %w", err)
	}
	return resOK, nil
}
