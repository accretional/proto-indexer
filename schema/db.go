package schema

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	sqliteembed "github.com/accretional/proto-sqlite/sqlite"
	sqlitepb "github.com/accretional/proto-sqlite/sqlite/pb"
)

// DB wraps a proto-sqlite Server bound to a specific SQLite file. Every
// Exec/Query call spawns a fresh sqlite3 process against DB.path — there
// is no long-lived connection. Callers that want atomicity across many
// INSERTs must send one Exec with BEGIN; …; COMMIT;.
type DB struct {
	srv  *sqliteembed.Server
	path string
}

var (
	sharedSrvOnce sync.Once
	sharedSrv     *sqliteembed.Server
)

// Server returns a process-wide sqliteembed.Server. Safe for concurrent
// use — the server itself is stateless beyond the embedded-binary extract
// cache inside the package.
func Server() *sqliteembed.Server {
	sharedSrvOnce.Do(func() { sharedSrv = sqliteembed.NewServer() })
	return sharedSrv
}

// Attach returns a DB bound to an existing file at path. It does not
// remove or initialize the file. Use this for read-only access from
// tests or tools.
func Attach(path string) *DB {
	return &DB{srv: Server(), path: path}
}

// OpenDB deletes any file at path, then initializes a new SQLite DB with
// ddl applied. Since the indexer always re-indexes, there is no migration
// path.
func OpenDB(ctx context.Context, path, ddl string) (*DB, error) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("schema: remove %s: %w", path, err)
	}
	d := &DB{srv: Server(), path: path}
	if err := d.Exec(ctx, ddl); err != nil {
		return nil, fmt.Errorf("schema: apply DDL to %s: %w", path, err)
	}
	return d, nil
}

// Path returns the on-disk file backing this DB.
func (d *DB) Path() string { return d.path }

// Exec runs sql (possibly multi-statement) with positional ? params.
// Response rows are discarded.
func (d *DB) Exec(ctx context.Context, sql string, params ...*sqlitepb.Value) error {
	_, err := d.srv.Query(ctx, &sqlitepb.QueryRequest{
		DbPath: d.path,
		Body:   &sqlitepb.QueryRequest_Sql{Sql: sql},
		Param:  params,
	})
	return err
}

// Query runs sql and returns the full response.
func (d *DB) Query(ctx context.Context, sql string, params ...*sqlitepb.Value) (*sqlitepb.QueryResponse, error) {
	return d.srv.Query(ctx, &sqlitepb.QueryRequest{
		DbPath: d.path,
		Body:   &sqlitepb.QueryRequest_Sql{Sql: sql},
		Param:  params,
	})
}

// QueryOne runs sql and returns the first row, erroring if the query
// produced no rows.
func (d *DB) QueryOne(ctx context.Context, sql string, params ...*sqlitepb.Value) (*sqlitepb.Row, error) {
	resp, err := d.Query(ctx, sql, params...)
	if err != nil {
		return nil, err
	}
	if len(resp.Row) == 0 {
		return nil, fmt.Errorf("schema: query returned no rows: %s", sql)
	}
	return resp.Row[0], nil
}

// Value constructors.

func Text(s string) *sqlitepb.Value {
	return &sqlitepb.Value{V: &sqlitepb.Value_Text{Text: s}}
}

func Int(n int64) *sqlitepb.Value {
	return &sqlitepb.Value{V: &sqlitepb.Value_Integer{Integer: n}}
}

func Real(f float64) *sqlitepb.Value {
	return &sqlitepb.Value{V: &sqlitepb.Value_Real{Real: f}}
}

func Blob(b []byte) *sqlitepb.Value {
	return &sqlitepb.Value{V: &sqlitepb.Value_Blob{Blob: b}}
}

// NullOr returns Null when b is nil, otherwise Blob(b).
func NullOrBlob(b []byte) *sqlitepb.Value {
	if b == nil {
		return Null()
	}
	return Blob(b)
}

// NullOrInt returns Null when n == 0 (matches prior nullInt() convention
// in the proto indexer where a zero line number means "unknown").
func NullOrInt(n int) *sqlitepb.Value {
	if n == 0 {
		return Null()
	}
	return Int(int64(n))
}

func Null() *sqlitepb.Value {
	return &sqlitepb.Value{V: &sqlitepb.Value_Null{Null: true}}
}

// Cell decoders. All take a row and a cell index.

func CellInt(row *sqlitepb.Row, i int) (int64, error) {
	if i >= len(row.Cell) {
		return 0, fmt.Errorf("schema: cell %d out of range (%d cells)", i, len(row.Cell))
	}
	if i < len(row.CellNull) && row.CellNull[i] {
		return 0, nil
	}
	return strconv.ParseInt(string(row.Cell[i]), 10, 64)
}

func CellText(row *sqlitepb.Row, i int) string {
	if i >= len(row.Cell) {
		return ""
	}
	if i < len(row.CellNull) && row.CellNull[i] {
		return ""
	}
	return string(row.Cell[i])
}

func CellBlob(row *sqlitepb.Row, i int) []byte {
	if i >= len(row.Cell) {
		return nil
	}
	if i < len(row.CellNull) && row.CellNull[i] {
		return nil
	}
	return row.Cell[i]
}
