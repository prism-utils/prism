package merge

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// DuckDBCaps configures DuckDB threads and memory_limit on connector open.
type DuckDBCaps struct {
	Threads     int
	MemoryLimit string
}

func newInMemoryConnector(caps DuckDBCaps) (*duckdb.Connector, error) {
	return duckdb.NewConnector("", duckdbInitFn(caps))
}

func duckdbInitFn(caps DuckDBCaps) func(driver.ExecerContext) error {
	if caps.Threads <= 0 && caps.MemoryLimit == "" {
		return nil
	}
	threads := caps.Threads
	memLimit := caps.MemoryLimit
	return func(exec driver.ExecerContext) error {
		ctx := context.Background()
		if threads > 0 {
			q := fmt.Sprintf("SET threads=%d", threads)
			if _, err := exec.ExecContext(ctx, q, nil); err != nil {
				return fmt.Errorf("merge: set threads: %w", err)
			}
		}
		if memLimit != "" {
			q := fmt.Sprintf("SET memory_limit='%s'", strings.ReplaceAll(memLimit, "'", "''"))
			if _, err := exec.ExecContext(ctx, q, nil); err != nil {
				return fmt.Errorf("merge: set memory_limit: %w", err)
			}
		}
		return nil
	}
}
