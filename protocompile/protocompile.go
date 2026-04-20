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
//   - canonical proto paths (relative to their natural root, not the repo root)
//   - import roots (-I flags) — the unique natural roots found
//
// The "natural root" of a file is its top-level containing directory (the
// first path component). For example, "proto/foo/bar.proto" has natural root
// "proto" and canonical name "foo/bar.proto". Files directly at the repo root
// have natural root ".".
func discover(repoPath string) (protos, roots []string, err error) {
	rootSet := map[string]bool{}
	sep := string(filepath.Separator)

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
		// Split into (naturalRoot, canonical): "proto/foo/bar.proto" → ("proto", "foo/bar.proto").
		var root, canonical string
		if idx := strings.Index(rel, sep); idx != -1 {
			root = rel[:idx]
			canonical = rel[idx+len(sep):]
		} else {
			root = "."
			canonical = rel
		}
		rootSet[root] = true
		protos = append(protos, canonical)
		return nil
	})
	if werr != nil {
		return nil, nil, fmt.Errorf("protocompile: walk: %w", werr)
	}
	sort.Strings(protos)

	for r := range rootSet {
		roots = append(roots, r)
	}
	sort.Strings(roots)
	return protos, roots, nil
}
