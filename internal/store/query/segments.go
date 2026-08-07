package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/segformat"
)

// metricsSource is one hot/tier file contributing to the sandbox metrics view.
type metricsSource struct {
	Path  string
	Alias string // set for duckdb after ATTACH; empty for parquet
}

func collectMetricsSources(tenantRoot string, hotOnly bool) ([]metricsSource, error) {
	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		return nil, err
	}
	if resolved, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = resolved
	}
	absRoot = filepath.Clean(absRoot)

	var out []metricsSource
	for _, rel := range []string{"hot/current.parquet", "hot/current.duckdb"} {
		p := filepath.Join(tenantRoot, rel)
		ok, err := safeTenantSegmentFile(absRoot, p)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, metricsSource{Path: p})
		}
	}
	if hotOnly {
		return out, nil
	}
	for tier := 0; tier < maxTier; tier++ {
		dir := filepath.Join(tenantRoot, "tiers", fmt.Sprintf("L%d", tier))
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
				continue
			}
			ext := filepath.Ext(e.Name())
			if ext != ".parquet" && ext != ".duckdb" {
				continue
			}
			p := filepath.Join(dir, e.Name())
			ok, err := safeTenantSegmentFile(absRoot, p)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, metricsSource{Path: p})
			}
		}
	}
	return out, nil
}

func attachMetricsDuckDB(ctx context.Context, conn *sql.Conn, sources []metricsSource) error {
	for i := range sources {
		if !segformat.IsDuckDB(sources[i].Path) {
			continue
		}
		alias := fmt.Sprintf("mseg_%d", i)
		q := fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", layout.ToSlash(sources[i].Path), alias)
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("attach metrics %s: %w", sources[i].Path, err)
		}
		sources[i].Alias = alias
	}
	return nil
}

func sandboxMetricsUnionSQLFromSources(sources []metricsSource) (string, error) {
	if len(sources) == 0 {
		return emptyMetricsViewSQL, nil
	}
	var parts []string
	for _, s := range sources {
		switch {
		case s.Alias != "":
			parts = append(parts, fmt.Sprintf(
				`SELECT "__name__", labels, value, timestamp_ms, ts FROM %s.%s`,
				s.Alias, segformat.MetricsTable,
			))
		default:
			parts = append(parts, fmt.Sprintf(
				`SELECT "__name__", labels, value, timestamp_ms, ts FROM read_parquet('%s')`,
				layout.ToSlash(s.Path),
			))
		}
	}
	sqlText := strings.Join(parts, " UNION ALL ")
	if !AssertNoUnionByName(sqlText) {
		return "", fmt.Errorf("view SQL must not use union_by_name or filename")
	}
	return sqlText, nil
}

// sandboxMetricsUnionSQL builds a metrics UNION ALL from parquet sources under
// tenantRoot (hot and optionally tiers). .duckdb paths are omitted because they
// need ATTACH aliases before projection.
func sandboxMetricsUnionSQL(tenantRoot string, hotOnly bool) (string, error) {
	sources, err := collectMetricsSources(tenantRoot, hotOnly)
	if err != nil {
		return "", err
	}
	var parquetOnly []metricsSource
	for _, s := range sources {
		if segformat.IsParquet(s.Path) {
			parquetOnly = append(parquetOnly, s)
		}
	}
	return sandboxMetricsUnionSQLFromSources(parquetOnly)
}

func safeTenantSegmentFile(absTenantRoot, path string) (bool, error) {
	return safeTenantParquetFile(absTenantRoot, path)
}

func listLogSegmentFiles(tenantRoot string) ([]logFileMeta, error) {
	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		return nil, err
	}
	if resolved, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = resolved
	}
	absRoot = filepath.Clean(absRoot)
	return globalLogsMetaCache.getOrScan(absRoot)
}

func attachLogsDuckDB(ctx context.Context, conn *sql.Conn, files []logFileMeta) error {
	for i, f := range files {
		if !segformat.IsDuckDB(f.Path) {
			continue
		}
		alias := fmt.Sprintf("lseg_%d", i)
		q := fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", layout.ToSlash(f.Path), alias)
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("attach logs %s: %w", f.Path, err)
		}
		files[i].duckAlias = alias
		rel := segformat.LogsRelationForPath(f.Path)
		//nolint:gosec // G201: alias/rel are server-assigned; path already tenant-validated.
		desc := fmt.Sprintf(
			`SELECT COUNT(*) > 0 FROM (DESCRIBE SELECT * FROM %s.%s) WHERE column_name = '%s'`,
			alias, rel, lokiTSColumn,
		)
		var has bool
		if err := conn.QueryRowContext(ctx, desc).Scan(&has); err != nil {
			return fmt.Errorf("describe logs %s: %w", f.Path, err)
		}
		files[i].HasIngestTS = has
	}
	return nil
}

func buildLogsRelationSQLMixed(files []logFileMeta, opts logsCatalogOpts) (string, error) {
	if len(files) == 0 {
		if opts.WithIngestTS {
			return emptyLokiLogsViewSQL, nil
		}
		return emptyLogsViewSQL, nil
	}
	var parquet []logFileMeta
	var duck []logFileMeta
	for _, f := range files {
		if segformat.IsDuckDB(f.Path) {
			duck = append(duck, f)
		} else {
			parquet = append(parquet, f)
		}
	}

	var parts []string
	if len(parquet) > 0 {
		parts = append(parts, buildLogsRelationSQL(parquet, opts))
	}
	for _, f := range duck {
		alias := f.duckAlias
		if alias == "" {
			return "", fmt.Errorf("query: duckdb log segment missing attach alias: %s", f.Path)
		}
		rel := segformat.LogsRelationForPath(f.Path)
		sel := fmt.Sprintf("SELECT * FROM %s.%s", alias, rel)
		if opts.WithIngestTS {
			if f.HasIngestTS {
				sel = fmt.Sprintf(
					`SELECT s.* EXCLUDE (%s), COALESCE(s.%s, %d::BIGINT) AS %s FROM %s.%s AS s`,
					lokiTSColumn, lokiTSColumn, f.MinTsNs, lokiTSColumn, alias, rel,
				)
			} else {
				sel = fmt.Sprintf(
					`SELECT *, %d::BIGINT AS %s FROM %s.%s`,
					f.MinTsNs, lokiTSColumn, alias, rel,
				)
			}
		}
		if opts.OmitMessage {
			sel = fmt.Sprintf(`SELECT * EXCLUDE (%s) FROM (%s)`, lokiMessageColumn, sel)
		}
		parts = append(parts, sel)
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return strings.Join(parts, " UNION ALL BY NAME "), nil
}
