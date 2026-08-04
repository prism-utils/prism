package query

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/elk-utilities/prism/internal/store/layout"
)

// lokiTSColumn is the synthetic ingest-time column projected onto every log row.
// The prism output contract forbids event timestamps inside a logs Parquet, so a
// window's landing time — its file mtime — is the only honest time axis, and it
// is broadcast to every row that file holds.
const lokiTSColumn = "__prism_ts_ns"

// lokiLineColumns are the columns that can carry the log line text, in the order
// they are preferred: an explicit message, else the mined template.
var lokiLineColumns = []string{"message", "template"}

// emptyLokiLogsViewSQL is the body of the logs relation when a tenant has no
// landed log parquet: zero rows with the guaranteed logs columns plus the summary
// columns and the ingest-time column, so a query against a logs-empty tenant
// returns an empty result instead of failing.
const emptyLokiLogsViewSQL = `SELECT ` +
	`CAST(NULL AS VARCHAR) AS message, ` +
	`CAST(NULL AS VARCHAR) AS format, ` +
	`CAST(NULL AS VARCHAR) AS template, ` +
	`CAST(NULL AS BIGINT) AS count, ` +
	`CAST(NULL AS BIGINT) AS ` + lokiTSColumn + ` ` +
	`WHERE 1=0`

// sandboxLokiLogsSQL builds a logs relation over the tenant's landed log parquet
// where every row carries its file's landing time in nanoseconds. Files are
// unified by column name — logs have a variable, per-format schema, and missing
// columns must NULL-fill rather than fail the union.
func sandboxLokiLogsSQL(tenantRoot string) (string, error) {
	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = resolved
	}
	absRoot = filepath.Clean(absRoot)

	paths, err := collectSafeLogParquetPaths(absRoot)
	if err != nil {
		return "", err
	}
	if len(paths) == 0 {
		return emptyLokiLogsViewSQL, nil
	}
	parts := make([]string, 0, len(paths))
	for _, p := range paths {
		fi, statErr := os.Stat(p) //nolint:gosec // G703: p comes from tenant-scoped globs already validated to resolve inside the tenant root
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue
			}
			return "", fmt.Errorf("query: stat log window: %w", statErr)
		}
		parts = append(parts, fmt.Sprintf(
			`SELECT *, %d::BIGINT AS %s FROM read_parquet('%s')`,
			fi.ModTime().UnixNano(), lokiTSColumn, escapeSQLLiteral(layout.ToSlash(p)),
		))
	}
	if len(parts) == 0 {
		return emptyLokiLogsViewSQL, nil
	}
	return strings.Join(parts, " UNION ALL BY NAME "), nil
}
