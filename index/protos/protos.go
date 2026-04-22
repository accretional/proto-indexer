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

// Index groups fds by package, writing one row per package into packagesPath
// (with a per-package FileDescriptorSet blob) and one row per top-level
// message/enum/service and per service method into symbolsPath. The two DBs
// are correlated by proto_package text, not by FK. repoLabel (owner/name)
// and repoURL (clone/origin URL) are stored on every row.
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
		pkgW.add(pkg, len(files), pkgBlob)
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

// pkgWriter accumulates packages rows and flushes them in one transaction.
type pkgWriter struct {
	ctx       context.Context
	db        *schema.DB
	repoLabel string
	repoURL   string
	rows      []pkgRow
}

type pkgRow struct {
	pkg       string
	fileCount int
	blob      []byte
}

func newPkgWriter(ctx context.Context, db *schema.DB, repoLabel, repoURL string) *pkgWriter {
	return &pkgWriter{ctx: ctx, db: db, repoLabel: repoLabel, repoURL: repoURL}
}

func (w *pkgWriter) add(pkg string, fileCount int, blob []byte) {
	w.rows = append(w.rows, pkgRow{pkg, fileCount, blob})
}

func (w *pkgWriter) flush() error {
	if len(w.rows) == 0 {
		return nil
	}
	tx, err := w.db.Begin(w.ctx)
	if err != nil {
		return fmt.Errorf("protos: begin package tx: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(w.ctx, `INSERT INTO packages(repo, repo_url, proto_package, file_count, descriptor_set) VALUES (?,?,?,?,?)`)
	if err != nil {
		return fmt.Errorf("protos: prepare package insert: %w", err)
	}
	defer stmt.Close()
	for _, r := range w.rows {
		if _, err := stmt.ExecContext(w.ctx, w.repoLabel, w.repoURL, r.pkg, r.fileCount, r.blob); err != nil {
			return fmt.Errorf("protos: insert package %s: %w", r.pkg, err)
		}
	}
	return tx.Commit()
}

// symWriter accumulates symbol rows and flushes them in one transaction.
type symWriter struct {
	ctx       context.Context
	db        *schema.DB
	repoLabel string
	repoURL   string
	rows      []symRow
}

type symRow struct {
	pkg, kind, name, fqn, filePath string
	line                           int
	descriptor                     []byte
	inputFQN, outputFQN            string
}

func newSymWriter(ctx context.Context, db *schema.DB, repoLabel, repoURL string) *symWriter {
	return &symWriter{ctx: ctx, db: db, repoLabel: repoLabel, repoURL: repoURL}
}

func (w *symWriter) add(pkg, kind, name, fqn, filePath string, line int, descriptor []byte, inputFQN, outputFQN string) {
	w.rows = append(w.rows, symRow{pkg, kind, name, fqn, filePath, line, descriptor, inputFQN, outputFQN})
}

func (w *symWriter) flush() error {
	if len(w.rows) == 0 {
		return nil
	}
	tx, err := w.db.Begin(w.ctx)
	if err != nil {
		return fmt.Errorf("protos: begin symbol tx: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(w.ctx, `INSERT INTO symbols(repo, repo_url, proto_package, kind, name, fqn, file_path, line, descriptor, input_fqn, output_fqn) VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return fmt.Errorf("protos: prepare symbol insert: %w", err)
	}
	defer stmt.Close()
	for _, r := range w.rows {
		line := any(r.line)
		if r.line == 0 {
			line = nil
		}
		if _, err := stmt.ExecContext(w.ctx, w.repoLabel, w.repoURL, r.pkg, r.kind, r.name, r.fqn, r.filePath, line, r.descriptor, textOrNull(r.inputFQN), textOrNull(r.outputFQN)); err != nil {
			return fmt.Errorf("protos: insert symbol %s: %w", r.fqn, err)
		}
	}
	return tx.Commit()
}

func textOrNull(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func indexFile(w *symWriter, pkg string, f *descriptorpb.FileDescriptorProto) error {
	lines := buildLineMap(f)
	filePath := f.GetName()

	for i, m := range f.MessageType {
		blob, err := proto.Marshal(m)
		if err != nil {
			return fmt.Errorf("protos: marshal message %s: %w", m.GetName(), err)
		}
		w.add(pkg, "message", m.GetName(), fqn(pkg, m.GetName()),
			filePath, lines[key(tagMessage, i)], blob, "", "")
	}
	for i, e := range f.EnumType {
		blob, err := proto.Marshal(e)
		if err != nil {
			return fmt.Errorf("protos: marshal enum %s: %w", e.GetName(), err)
		}
		w.add(pkg, "enum", e.GetName(), fqn(pkg, e.GetName()),
			filePath, lines[key(tagEnum, i)], blob, "", "")
	}
	for i, s := range f.Service {
		blob, err := proto.Marshal(s)
		if err != nil {
			return fmt.Errorf("protos: marshal service %s: %w", s.GetName(), err)
		}
		w.add(pkg, "service", s.GetName(), fqn(pkg, s.GetName()),
			filePath, lines[key(tagService, i)], blob, "", "")
		for j, m := range s.Method {
			mblob, err := proto.Marshal(m)
			if err != nil {
				return fmt.Errorf("protos: marshal method %s.%s: %w", s.GetName(), m.GetName(), err)
			}
			methodFQN := fqn(pkg, s.GetName()) + "." + m.GetName()
			w.add(pkg, "method", m.GetName(), methodFQN,
				filePath, lines[key(tagService, i, tagMethod, j)], mblob,
				strings.TrimPrefix(m.GetInputType(), "."),
				strings.TrimPrefix(m.GetOutputType(), "."),
			)
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
