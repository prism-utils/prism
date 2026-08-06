//go:build cgo

// Package duckdb implements an Encoder that seals an Arrow RecordBatch into a
// checkpointed single-table .duckdb file (STORAGE_VERSION pinned).
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/marcboeker/go-duckdb/v2"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/duckdbfile"
)

// Type is the config identifier for this encoder.
const Type = "duckdb"

// Config configures the duckdb encoder.
type Config struct {
	// StorageVersion pins STORAGE_VERSION on created files. Empty uses the
	// shared default matching the store's go-duckdb line.
	StorageVersion string `json:"storage_version"`
}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns the duckdb encoder factory.
func NewFactory() component.Factory[component.Encoder] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Encoder, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("encoder/duckdb: unexpected config type %T", cfg)
	}
	sv := c.StorageVersion
	if sv == "" {
		sv = duckdbfile.DefaultStorageVersion
	}
	return &encoder{storageVersion: sv}, nil
}

type encoder struct {
	storageVersion string
}

func (*encoder) Start(context.Context, component.Host) error { return nil }
func (*encoder) Shutdown(context.Context) error              { return nil }

func (e *encoder) Encode(ctx context.Context, in data.RecordBatch) (data.EncodedBlock, error) {
	defer in.Release()
	rec := in.Record()
	if rec == nil || rec.NumRows() == 0 {
		return data.EncodedBlock{Format: Type, Rows: 0}, nil
	}

	dir, err := os.MkdirTemp("", "prism-duckdb-enc-*")
	if err != nil {
		return data.EncodedBlock{}, fmt.Errorf("encoder/duckdb: temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "window.duckdb")

	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return data.EncodedBlock{}, fmt.Errorf("encoder/duckdb: connector: %w", err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	sv := strings.ReplaceAll(e.storageVersion, "'", "''")
	attach := fmt.Sprintf("ATTACH '%s' AS exp (STORAGE_VERSION '%s')", filepath.ToSlash(path), sv)
	if _, err := db.ExecContext(ctx, attach); err != nil {
		return data.EncodedBlock{}, fmt.Errorf("encoder/duckdb: attach: %w", err)
	}

	createSQL, err := createTableSQL(rec.Schema())
	if err != nil {
		_, _ = db.ExecContext(ctx, "DETACH exp")
		return data.EncodedBlock{}, err
	}
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		_, _ = db.ExecContext(ctx, "DETACH exp")
		return data.EncodedBlock{}, fmt.Errorf("encoder/duckdb: create table: %w", err)
	}

	if err := insertSQL(ctx, db, rec); err != nil {
		_, _ = db.ExecContext(ctx, "DETACH exp")
		return data.EncodedBlock{}, err
	}
	if _, err := db.ExecContext(ctx, "CHECKPOINT exp"); err != nil {
		_, _ = db.ExecContext(ctx, "DETACH exp")
		return data.EncodedBlock{}, fmt.Errorf("encoder/duckdb: checkpoint: %w", err)
	}
	if _, err := db.ExecContext(ctx, "DETACH exp"); err != nil {
		return data.EncodedBlock{}, fmt.Errorf("encoder/duckdb: detach: %w", err)
	}
	_ = os.Remove(path + ".wal")

	out, err := os.ReadFile(path)
	if err != nil {
		return data.EncodedBlock{}, fmt.Errorf("encoder/duckdb: read: %w", err)
	}
	return data.EncodedBlock{Format: Type, Bytes: out, Rows: int(rec.NumRows())}, nil
}

func createTableSQL(schema *arrow.Schema) (string, error) {
	cols := make([]string, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		dt, err := duckType(f.Type)
		if err != nil {
			return "", err
		}
		cols[i] = fmt.Sprintf("%s %s", quoteIdent(f.Name), dt)
	}
	return fmt.Sprintf("CREATE TABLE exp.%s (%s)", duckdbfile.Table, strings.Join(cols, ", ")), nil
}

func duckType(dt arrow.DataType) (string, error) {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY:
		return "VARCHAR", nil
	case arrow.BOOL:
		return "BOOLEAN", nil
	case arrow.INT8, arrow.UINT8, arrow.INT16, arrow.UINT16, arrow.INT32, arrow.UINT32:
		return "INTEGER", nil
	case arrow.INT64, arrow.UINT64:
		return "BIGINT", nil
	case arrow.FLOAT32, arrow.FLOAT64:
		return "DOUBLE", nil
	case arrow.TIMESTAMP:
		return "TIMESTAMP", nil
	default:
		return "", fmt.Errorf("encoder/duckdb: unsupported arrow type %s", dt)
	}
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func insertSQL(ctx context.Context, db *sql.DB, rec arrow.RecordBatch) error {
	ncols := int(rec.NumCols())
	placeholders := make([]string, ncols)
	cols := make([]string, ncols)
	for c := 0; c < ncols; c++ {
		placeholders[c] = "?"
		cols[c] = quoteIdent(rec.ColumnName(c))
	}
	//nolint:gosec // G201: table is a package const; column idents are quoted from Arrow schema.
	q := fmt.Sprintf("INSERT INTO exp.%s (%s) VALUES (%s)",
		duckdbfile.Table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
	n := int(rec.NumRows())
	for i := 0; i < n; i++ {
		args := make([]any, ncols)
		for c := 0; c < ncols; c++ {
			args[c] = cellValue(rec.Column(c), i)
		}
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("encoder/duckdb: insert row %d: %w", i, err)
		}
	}
	return nil
}

func cellValue(col arrow.Array, row int) any {
	if col.IsNull(row) {
		return nil
	}
	switch c := col.(type) {
	case *array.String:
		return c.Value(row)
	case *array.LargeString:
		return c.Value(row)
	case *array.Binary:
		return string(c.Value(row))
	case *array.Boolean:
		return c.Value(row)
	case *array.Int8:
		return int64(c.Value(row))
	case *array.Int16:
		return int64(c.Value(row))
	case *array.Int32:
		return int64(c.Value(row))
	case *array.Int64:
		return c.Value(row)
	case *array.Uint8:
		return int64(c.Value(row))
	case *array.Uint16:
		return int64(c.Value(row))
	case *array.Uint32:
		return int64(c.Value(row))
	case *array.Uint64:
		return int64(c.Value(row)) //nolint:gosec // window values fit int64 for DuckDB BIGINT
	case *array.Float32:
		return float64(c.Value(row))
	case *array.Float64:
		return c.Value(row)
	case *array.Timestamp:
		return c.Value(row).ToTime(c.DataType().(*arrow.TimestampType).Unit)
	default:
		return col.ValueStr(row)
	}
}
