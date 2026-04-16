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
//   - relative paths of .proto files (relative to repoPath)
//   - import roots, ordered: repo root first, then any directory that directly
//     contains a .proto file (so imports like "foo/bar.proto" can resolve
//     whether files live at repo root, under proto/, api/, pkg/proto/, etc.)
func discover(repoPath string) (protos, roots []string, err error) {
	rootSet := map[string]bool{".": true}
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
		protos = append(protos, rel)
		if dir := filepath.Dir(rel); dir != "." {
			rootSet[dir] = true
		}
		return nil
	})
	if werr != nil {
		return nil, nil, fmt.Errorf("protocompile: walk: %w", werr)
	}
	sort.Strings(protos)

	// Add each containing dir AND each of its ancestors up to repo root.
	// This lets imports like "subpkg/foo.proto" resolve when the importing
	// file lives several dirs deep.
	expanded := map[string]bool{}
	for d := range rootSet {
		for cur := d; ; cur = filepath.Dir(cur) {
			expanded[cur] = true
			if cur == "." || cur == "/" {
				break
			}
		}
	}
	for d := range expanded {
		roots = append(roots, d)
	}
	sort.Strings(roots)
	return protos, roots, nil
}
