// Package protos indexes a FileDescriptorSet into two SQLite DBs: one
// for packages (with per-package FileDescriptorSet blobs) and one for
// symbols. The split lets remote clients fetch the small symbols DB
// without pulling the large FDS blobs unless they actually need them.
package protos

import (
	"context"
	"fmt"
	"strings"

	"github.com/accretional/proto-indexer/schema"
	sqlitepb "github.com/accretional/proto-sqlite/sqlite/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Paths in SourceCodeInfo.Location.Path follow the tag numbers of the parent
// field in FileDescriptorProto / DescriptorProto. These are the top-level
// entries we care about.
const (
	tagMessage int = 4 // FileDescriptorProto.message_type
	tagEnum    int = 5 // FileDescriptorProto.enum_type
	tagService int = 6 // FileDescriptorProto.service
	tagMethod  int = 2 // ServiceDescriptorProto.method
)

// Batched-flush size budget; see source package for the ARG_MAX rationale.
const maxChunkBytes = 500 * 1024

// Index groups fds by package, writing one row per package into packagesPath
// (with a per-package FileDescriptorSet blob) and one row per top-level
// message/enum/service and per service method into symbolsPath. The two DBs
// are correlated by proto_package text, not by FK. repoLabel (owner/name)
// and repoURL (clone/origin URL) are stored on every row.
//
// Unlike the previous database/sql implementation, errors do not roll back
// across DBs: proto-sqlite's process-spawn model has no cross-call
// transactions. On partial failure the packages DB may be fully written
// while symbols is empty or partial. Callers (processRepo) treat the repo
// as failed and re-run from scratch, which is safe since Index starts from
// a fresh OpenDB every time.
func Index(ctx context.Context, fds *descriptorpb.FileDescriptorSet, repoLabel, repoURL, packagesPath, symbolsPath string) error {
	pkgDB, err := schema.OpenDB(ctx, packagesPath, schema.PackagesDDL)
	if err != nil {
		return err
	}
	symDB, err := schema.OpenDB(ctx, symbolsPath, schema.SymbolsDDL)
	if err != nil {
		return err
	}

	byPkg := map[string][]*descriptorpb.FileDescriptorProto{}
	for _, f := range fds.GetFile() {
		byPkg[f.GetPackage()] = append(byPkg[f.GetPackage()], f)
	}

	pkgW := newPkgWriter(ctx, pkgDB, repoLabel, repoURL)
	symW := newSymWriter(ctx, symDB, repoLabel, repoURL)

	for pkg, files := range byPkg {
		pkgFDS := &descriptorpb.FileDescriptorSet{File: files}
		pkgBlob, err := proto.Marshal(pkgFDS)
		if err != nil {
			return fmt.Errorf("protos: marshal package %q: %w", pkg, err)
		}
		if err := pkgW.add(pkg, len(files), pkgBlob); err != nil {
			return err
		}
		for _, f := range files {
			if err := indexFile(symW, pkg, f); err != nil {
				return err
			}
		}
	}
	if err := pkgW.flush(); err != nil {
		return err
	}
	return symW.flush()
}

// pkgWriter batches INSERTs into packages.sqlite.
type pkgWriter struct {
	ctx       context.Context
	db        *schema.DB
	repoLabel string
	repoURL   string
	params    []*sqlitepb.Value
	rows      int
	bytes     int
}

func newPkgWriter(ctx context.Context, db *schema.DB, repoLabel, repoURL string) *pkgWriter {
	return &pkgWriter{ctx: ctx, db: db, repoLabel: repoLabel, repoURL: repoURL}
}

func (w *pkgWriter) add(pkg string, fileCount int, blob []byte) error {
	rowBytes := 32 + len(pkg) + 2*len(blob)
	if w.bytes+rowBytes > maxChunkBytes && w.rows > 0 {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.params = append(w.params,
		schema.Text(w.repoLabel), schema.Text(w.repoURL),
		schema.Text(pkg), schema.Int(int64(fileCount)), schema.Blob(blob))
	w.rows++
	w.bytes += rowBytes
	return nil
}

func (w *pkgWriter) flush() error {
	if w.rows == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("BEGIN;\nINSERT INTO packages(repo, repo_url, proto_package, file_count, descriptor_set) VALUES ")
	for i := 0; i < w.rows; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?,?)")
	}
	sb.WriteString(";\nCOMMIT;")
	if err := w.db.Exec(w.ctx, sb.String(), w.params...); err != nil {
		return fmt.Errorf("protos: flush %d package rows: %w", w.rows, err)
	}
	w.params = w.params[:0]
	w.rows = 0
	w.bytes = 0
	return nil
}

// symWriter batches INSERTs into symbols.sqlite.
type symWriter struct {
	ctx       context.Context
	db        *schema.DB
	repoLabel string
	repoURL   string
	params    []*sqlitepb.Value
	rows      int
	bytes     int
}

func newSymWriter(ctx context.Context, db *schema.DB, repoLabel, repoURL string) *symWriter {
	return &symWriter{ctx: ctx, db: db, repoLabel: repoLabel, repoURL: repoURL}
}

func (w *symWriter) add(pkg, kind, name, fqn, filePath string, line int, descriptor []byte, inputFQN, outputFQN string) error {
	rowBytes := 64 + len(pkg) + len(kind) + len(name) + len(fqn) + len(filePath) + 2*len(descriptor) + len(inputFQN) + len(outputFQN)
	if w.bytes+rowBytes > maxChunkBytes && w.rows > 0 {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.params = append(w.params,
		schema.Text(w.repoLabel), schema.Text(w.repoURL),
		schema.Text(pkg), schema.Text(kind), schema.Text(name),
		schema.Text(fqn), schema.Text(filePath), schema.NullOrInt(line),
		schema.Blob(descriptor),
		textOrNull(inputFQN), textOrNull(outputFQN),
	)
	w.rows++
	w.bytes += rowBytes
	return nil
}

func (w *symWriter) flush() error {
	if w.rows == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("BEGIN;\nINSERT INTO symbols(repo, repo_url, proto_package, kind, name, fqn, file_path, line, descriptor, input_fqn, output_fqn) VALUES ")
	for i := 0; i < w.rows; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?,?,?,?,?,?,?,?)")
	}
	sb.WriteString(";\nCOMMIT;")
	if err := w.db.Exec(w.ctx, sb.String(), w.params...); err != nil {
		return fmt.Errorf("protos: flush %d symbol rows: %w", w.rows, err)
	}
	w.params = w.params[:0]
	w.rows = 0
	w.bytes = 0
	return nil
}

func textOrNull(s string) *sqlitepb.Value {
	if s == "" {
		return schema.Null()
	}
	return schema.Text(s)
}

func indexFile(w *symWriter, pkg string, f *descriptorpb.FileDescriptorProto) error {
	lines := buildLineMap(f)
	filePath := f.GetName()

	for i, m := range f.MessageType {
		blob, err := proto.Marshal(m)
		if err != nil {
			return fmt.Errorf("protos: marshal message %s: %w", m.GetName(), err)
		}
		if err := w.add(pkg, "message", m.GetName(), fqn(pkg, m.GetName()),
			filePath, lines[key(tagMessage, i)], blob, "", ""); err != nil {
			return err
		}
	}
	for i, e := range f.EnumType {
		blob, err := proto.Marshal(e)
		if err != nil {
			return fmt.Errorf("protos: marshal enum %s: %w", e.GetName(), err)
		}
		if err := w.add(pkg, "enum", e.GetName(), fqn(pkg, e.GetName()),
			filePath, lines[key(tagEnum, i)], blob, "", ""); err != nil {
			return err
		}
	}
	for i, s := range f.Service {
		blob, err := proto.Marshal(s)
		if err != nil {
			return fmt.Errorf("protos: marshal service %s: %w", s.GetName(), err)
		}
		if err := w.add(pkg, "service", s.GetName(), fqn(pkg, s.GetName()),
			filePath, lines[key(tagService, i)], blob, "", ""); err != nil {
			return err
		}
		for j, m := range s.Method {
			mblob, err := proto.Marshal(m)
			if err != nil {
				return fmt.Errorf("protos: marshal method %s.%s: %w", s.GetName(), m.GetName(), err)
			}
			methodFQN := fqn(pkg, s.GetName()) + "." + m.GetName()
			if err := w.add(pkg, "method", m.GetName(), methodFQN,
				filePath, lines[key(tagService, i, tagMethod, j)], mblob,
				strings.TrimPrefix(m.GetInputType(), "."),
				strings.TrimPrefix(m.GetOutputType(), "."),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func fqn(pkg, name string) string {
	if pkg == "" {
		return name
	}
	return pkg + "." + name
}

func key(parts ...int) string {
	b := make([]byte, 0, len(parts)*3)
	for i, p := range parts {
		if i > 0 {
			b = append(b, '.')
		}
		b = appendInt(b, p)
	}
	return string(b)
}

func appendInt(b []byte, n int) []byte {
	if n == 0 {
		return append(b, '0')
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return append(b, buf[i:]...)
}

func buildLineMap(f *descriptorpb.FileDescriptorProto) map[string]int {
	out := map[string]int{}
	sci := f.GetSourceCodeInfo()
	if sci == nil {
		return out
	}
	for _, loc := range sci.Location {
		if len(loc.Span) < 1 {
			continue
		}
		parts := make([]int, len(loc.Path))
		for i, v := range loc.Path {
			parts[i] = int(v)
		}
		out[key(parts...)] = int(loc.Span[0]) + 1
	}
	return out
}
