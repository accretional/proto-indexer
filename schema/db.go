package schema

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

// DB wraps a SQLite database file with a persistent connection.
type DB struct {
	db   *sql.DB
	path string
}

// Row holds the scanned values of a single result row, indexed by position.
type Row []any

func openSQLite(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Attach returns a DB bound to an existing file at path. It does not
// remove or initialize the file. Use this for read access from tests or tools.
func Attach(path string) *DB {
	db, _ := openSQLite(path)
	return &DB{db: db, path: path}
}

// OpenDB deletes any file at path, then initializes a new SQLite DB with
// ddl applied. Since the indexer always re-indexes, there is no migration path.
func OpenDB(ctx context.Context, path, ddl string) (*DB, error) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("schema: remove %s: %w", path, err)
	}
	db, err := openSQLite(path)
	if err != nil {
		return nil, fmt.Errorf("schema: open %s: %w", path, err)
	}
	d := &DB{db: db, path: path}
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		db.Close()
		return nil, fmt.Errorf("schema: apply DDL to %s: %w", path, err)
	}
	return d, nil
}

// Path returns the on-disk file backing this DB.
func (d *DB) Path() string { return d.path }

// Begin starts a transaction for batch writes.
func (d *DB) Begin(ctx context.Context) (*sql.Tx, error) {
	return d.db.BeginTx(ctx, nil)
}

// Query runs sql and returns all result rows.
func (d *DB) Query(ctx context.Context, query string, params ...any) ([]Row, error) {
	rows, err := d.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var result []Row
	for rows.Next() {
		vals := make(Row, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		result = append(result, vals)
	}
	return result, rows.Err()
}

// QueryOne runs sql and returns the first row, erroring if no rows returned.
func (d *DB) QueryOne(ctx context.Context, query string, params ...any) (Row, error) {
	rows, err := d.Query(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("schema: query returned no rows: %s", query)
	}
	return rows[0], nil
}

// Cell decoders. All take a Row and a zero-based cell index.

func CellInt(row Row, i int) (int64, error) {
	if i >= len(row) {
		return 0, fmt.Errorf("schema: cell %d out of range (%d cells)", i, len(row))
	}
	if row[i] == nil {
		return 0, nil
	}
	switch v := row[i].(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	}
	return 0, fmt.Errorf("schema: cell %d: unexpected type %T", i, row[i])
}

func CellText(row Row, i int) string {
	if i >= len(row) || row[i] == nil {
		return ""
	}
	if s, ok := row[i].(string); ok {
		return s
	}
	return fmt.Sprintf("%v", row[i])
}

func CellBlob(row Row, i int) []byte {
	if i >= len(row) || row[i] == nil {
		return nil
	}
	b, _ := row[i].([]byte)
	return b
}
