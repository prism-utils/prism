package merge

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
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
	threads := caps.Threads
	memLimit := caps.MemoryLimit
	return func(exec driver.ExecerContext) error {
		ctx := context.Background()
		// In-memory DuckDB defaults `.tmp` to CWD; under read-only rootfs that
		// is `/` and merge fails. Always spill under TMPDIR (chart sets /tmp).
		tmp := os.TempDir()
		if tmp == "" {
			tmp = "/tmp"
		}
		q := fmt.Sprintf("SET temp_directory='%s'", strings.ReplaceAll(filepath.Join(tmp, "duckdb-merge"), "'", "''"))
		if _, err := exec.ExecContext(ctx, q, nil); err != nil {
			return fmt.Errorf("merge: set temp_directory: %w", err)
		}
		// High-cardinality COPY ORDER BY peaks without this; live-demo CH metrics
		// OOMed merge at 2.1Gi even with a 2300MB soft cap.
		if _, err := exec.ExecContext(ctx, "SET preserve_insertion_order=false", nil); err != nil {
			return fmt.Errorf("merge: set preserve_insertion_order: %w", err)
		}
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
