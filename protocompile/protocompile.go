// Package protocompile invokes protoc against a repo's .proto files
// and returns a FileDescriptorSet.
package protocompile

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// skipDirs are directory names never descended into when searching for protos.
var skipDirs = map[string]bool{
	".git":         true,
	"node_modules": true,
	"build":        true,
	"dist":         true,
}

// Compile locates .proto files under repoPath, invokes protoc with heuristic
// import roots, and returns the decoded FileDescriptorSet. If no .proto files
// are found, returns (nil, nil).
func Compile(ctx context.Context, repoPath string) (*descriptorpb.FileDescriptorSet, error) {
	protos, roots, err := discover(repoPath)
	if err != nil {
		return nil, err
	}
	if len(protos) == 0 {
		return nil, nil
	}

	tmp, err := os.CreateTemp("", "fds-*.pb")
	if err != nil {
		return nil, fmt.Errorf("protocompile: tempfile: %w", err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	args := []string{"--include_imports", "--include_source_info", "--descriptor_set_out=" + tmp.Name()}
	for _, r := range roots {
		args = append(args, "-I", r)
	}
	args = append(args, protos...)

	cmd := exec.CommandContext(ctx, "protoc", args...)
	cmd.Dir = repoPath
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("protoc failed: %w\n%s", err, out)
	}

	data, err := os.ReadFile(tmp.Name())
	if err != nil {
		return nil, fmt.Errorf("protocompile: read descriptor set: %w", err)
	}
	fds := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(data, fds); err != nil {
		return nil, fmt.Errorf("protocompile: unmarshal: %w", err)
	}
	return fds, nil
}

// discover walks repoPath and returns:
//   - input proto paths passed to protoc (full repo-relative paths)
//   - import roots (-I flags) chosen by a depth-based heuristic
//
// Root selection by file depth (number of path separators in the repo-relative
// path) and the spread of depth-1 files across directories:
//
//   - depth 0 (root-level): root "."
//   - depth 1, all in ONE directory: root = that directory
//     e.g. proto/foo.proto + proto/bar.proto → -I proto
//     Same-directory bare imports (import "bar.proto") resolve correctly.
//   - depth 1, spread across MULTIPLE directories: root "."
//     e.g. filer/plan92.proto + identifier/id.proto → -I .
//     Cross-directory imports (import "filer/plan92.proto") resolve correctly.
//   - depth 2: root = parent directory
//     e.g. proto/files/doc.proto → -I proto/files
//     Same-directory bare imports inside the leaf dir resolve correctly.
//   - depth 3+: root = first component
//     e.g. proto/gf/v1/axes.proto → -I proto
//     Vendor/googleapis convention: imports are relative to the top-level tree.
//
// Roots are sorted longest-first so protoc's first-match rule picks the most
// specific root for each file, preventing a shorter ancestor from shadowing a
// more specific one (e.g. -I proto/files wins over -I proto for depth-2 files).
//
// Input files come from "git ls-files" when available, so gitignored vendor
// trees are excluded from compilation inputs. The filesystem walk still adds
// roots from the full tree so transitive imports inside ignored directories
// (third_party/, protodeps/, etc.) resolve at compile time.
func discover(repoPath string) (protos, roots []string, err error) {
	rootSet := map[string]bool{}
	sep := string(filepath.Separator)
	var walkedProtos []string
	depth1Comps := map[string]bool{} // unique first-component dirs for depth-1 files

	// Full filesystem walk: collects import roots from the entire tree
	// (including gitignored vendor dirs) and builds a fallback proto list.
	werr := filepath.WalkDir(repoPath, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return nil
		}
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".proto") {
			return nil
		}
		rel, rerr := filepath.Rel(repoPath, path)
		if rerr != nil {
			return nil
		}
		depth := strings.Count(rel, sep)
		switch {
		case depth == 0:
			rootSet["."] = true
		case depth == 1:
			depth1Comps[rel[:strings.Index(rel, sep)]] = true
			// depth-1 root is resolved after the full walk (see below)
		case depth == 2:
			rootSet[rel[:strings.LastIndex(rel, sep)]] = true
		default:
			rootSet[rel[:strings.Index(rel, sep)]] = true
		}
		walkedProtos = append(walkedProtos, rel)
		return nil
	})
	if werr != nil {
		return nil, nil, fmt.Errorf("protocompile: walk: %w", werr)
	}

	// Resolve the root for depth-1 files now that we know all the directories.
	// One unique parent dir → same-directory bare imports work with that dir.
	// Multiple parent dirs → cross-directory imports need the repo root.
	switch len(depth1Comps) {
	case 1:
		for comp := range depth1Comps {
			rootSet[comp] = true
		}
	default:
		if len(depth1Comps) > 1 {
			rootSet["."] = true
		}
	}

	// Prefer "git ls-files" so gitignored vendor trees are excluded from
	// compilation inputs. Fall back to the full walked list if git is
	// unavailable or repoPath is not a git repository.
	if gitProtos, gitErr := gitTrackedProtos(repoPath); gitErr == nil {
		protos = gitProtos
	} else {
		protos = walkedProtos
	}
	sort.Strings(protos)

	for r := range rootSet {
		roots = append(roots, r)
	}
	// Longest roots first: protoc uses the first -I that is a path prefix of
	// the input file to determine the canonical import name. Sorting longest
	// first ensures the most specific (deepest) root wins over a shorter
	// ancestor that is also a prefix.
	sort.Slice(roots, func(i, j int) bool {
		if len(roots[i]) != len(roots[j]) {
			return len(roots[i]) > len(roots[j])
		}
		return roots[i] < roots[j]
	})
	return protos, roots, nil
}

// gitTrackedProtos runs "git ls-files -- *.proto" in repoPath and returns the
// repo-relative path of each tracked proto file (no stripping — full paths let
// protoc derive the canonical import name from the chosen -I root).
func gitTrackedProtos(repoPath string) ([]string, error) {
	cmd := exec.Command("git", "ls-files", "--", "*.proto")
	cmd.Dir = repoPath
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var files []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		files = append(files, filepath.FromSlash(line))
	}
	return files, nil
}
