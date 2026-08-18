package query

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/metricsmeta"
)

// metricsOpenOpts selects which metrics files a sandbox or structured query opens.
type metricsOpenOpts struct {
	HotOnly   bool
	Start     time.Time
	End       time.Time
	HotWindow time.Duration
	Now       time.Time
	ColdDir   string
}

// metricsFileMeta is one hot or tier segment the planners may open.
type metricsFileMeta struct {
	Path    string
	MinTsNs int64
	MaxTsNs int64
	hot     bool
}

const autoHotSkew = time.Minute

func collectMetricsSources(ctx context.Context, tenantRoot string, opts *metricsOpenOpts) ([]metricsSource, error) {
	if opts == nil {
		opts = &metricsOpenOpts{}
	}
	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		return nil, err
	}
	if resolved, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = resolved
	}
	absRoot = filepath.Clean(absRoot)

	files, err := listMetricsFiles(ctx, absRoot, opts.ColdDir)
	if err != nil {
		return nil, err
	}
	files = ensureHotFiles(ctx, absRoot, files)

	var hot, cold []metricsFileMeta
	for _, f := range files {
		if f.hot {
			if minNs, maxNs, ok := metricsmeta.FileBounds(ctx, f.Path); ok {
				f.MinTsNs, f.MaxTsNs = minNs, maxNs
			}
			hot = append(hot, f)
		} else {
			cold = append(cold, f)
		}
	}

	if opts.HotOnly {
		return metasToSources(hot), nil
	}

	startNs, endNs := opts.Start.UnixNano(), opts.End.UnixNano()
	hasRange := !opts.Start.IsZero() && opts.End.After(opts.Start)
	if hasRange && rangeInsideHot(opts.Start, opts.End, hot, opts) {
		return metasToSources(hot), nil
	}
	if hasRange {
		hot = filterMetricsFiles(hot, startNs, endNs)
		cold = filterMetricsFiles(cold, startNs, endNs)
	}
	out := make([]metricsFileMeta, 0, len(hot)+len(cold))
	out = append(out, hot...)
	out = append(out, cold...)
	return metasToSources(out), nil
}

func ensureHotFiles(ctx context.Context, absRoot string, files []metricsFileMeta) []metricsFileMeta {
	seen := map[string]struct{}{}
	for _, f := range files {
		if f.hot {
			seen[filepath.ToSlash(f.Path)] = struct{}{}
		}
	}
	for _, rel := range []string{"hot/current.parquet", "hot/current.duckdb"} {
		p := filepath.Join(absRoot, filepath.FromSlash(rel))
		ok, err := safeTenantSegmentFile(absRoot, p)
		if err != nil || !ok {
			continue
		}
		slash := filepath.ToSlash(p)
		if _, dup := seen[slash]; dup {
			continue
		}
		minNs, maxNs, bounded := metricsmeta.FileBounds(ctx, p)
		if !bounded {
			continue
		}
		files = append(files, metricsFileMeta{Path: p, MinTsNs: minNs, MaxTsNs: maxNs, hot: true})
	}
	return files
}

func metasToSources(files []metricsFileMeta) []metricsSource {
	out := make([]metricsSource, 0, len(files))
	for _, f := range files {
		out = append(out, metricsSource{Path: f.Path})
	}
	return out
}

func rangeInsideHot(start, end time.Time, hot []metricsFileMeta, opts *metricsOpenOpts) bool {
	hotMin, hotMax, ok := hotCoverage(hot, opts)
	if !ok {
		return false
	}
	s, e := start.UnixNano(), end.UnixNano()
	return s >= hotMin && e <= hotMax+autoHotSkew.Nanoseconds()
}

func hotCoverage(hot []metricsFileMeta, opts *metricsOpenOpts) (minNs, maxNs int64, ok bool) {
	var sawEmpty bool
	for _, f := range hot {
		if f.MinTsNs == 0 && f.MaxTsNs == 0 {
			sawEmpty = true
			continue
		}
		if !ok || f.MinTsNs < minNs {
			minNs = f.MinTsNs
		}
		if !ok || f.MaxTsNs > maxNs {
			maxNs = f.MaxTsNs
		}
		ok = true
	}
	if ok {
		return minNs, maxNs, true
	}
	if sawEmpty || len(hot) == 0 || opts.HotWindow <= 0 {
		return 0, 0, false
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return now.Add(-opts.HotWindow).UnixNano(), now.UnixNano(), true
}

func filterMetricsFiles(files []metricsFileMeta, startNs, endNs int64) []metricsFileMeta {
	if endNs <= startNs {
		return append([]metricsFileMeta(nil), files...)
	}
	out := make([]metricsFileMeta, 0, len(files))
	for _, f := range files {
		if f.MaxTsNs < startNs || f.MinTsNs >= endNs {
			continue
		}
		out = append(out, f)
	}
	return out
}

func listMetricsFiles(ctx context.Context, absRoot, coldDir string) ([]metricsFileMeta, error) {
	dataDir := filepath.Dir(absRoot)
	tenant := filepath.Base(absRoot)
	gen, err := metricsmeta.ReadGeneration(dataDir, tenant)
	if err != nil {
		return nil, err
	}
	m, err := metricsmeta.ReadManifest(dataDir, tenant)
	if err != nil {
		return nil, err
	}
	if m.Version == gen && len(m.Files) > 0 {
		if files, ok := manifestToMetricsFiles(dataDir, coldDir, tenant, m); ok {
			return files, nil
		}
	}
	rebuilt, err := metricsmeta.RebuildManifestRoots(ctx, dataDir, coldDir, tenant, gen)
	if err != nil {
		return nil, err
	}
	_ = metricsmeta.WriteManifest(dataDir, tenant, rebuilt)
	files, ok := manifestToMetricsFiles(dataDir, coldDir, tenant, rebuilt)
	if !ok {
		return scanMetricsFiles(ctx, dataDir, coldDir, tenant)
	}
	return files, nil
}

func manifestToMetricsFiles(dataDir, coldDir, tenant string, m metricsmeta.Manifest) ([]metricsFileMeta, bool) {
	roots := layout.AllowedTenantRoots(dataDir, coldDir, tenant)
	out := make([]metricsFileMeta, 0, len(m.Files))
	for _, f := range m.Files {
		abs, ok := layout.ResolveRel(dataDir, coldDir, tenant, f.Path)
		if !ok {
			return nil, false
		}
		allowed, err := safeTenantParquetInRoots(roots, abs)
		if err != nil || !allowed {
			return nil, false
		}
		out = append(out, metricsFileMeta{
			Path:    abs,
			MinTsNs: f.MinTsNs,
			MaxTsNs: f.MaxTsNs,
			hot:     isHotRel(f.Path),
		})
	}
	return out, true
}

func isHotRel(rel string) bool {
	s := filepath.ToSlash(rel)
	return s == "hot/current.parquet" || s == "hot/current.duckdb"
}

func scanMetricsFiles(ctx context.Context, dataDir, coldDir, tenant string) ([]metricsFileMeta, error) {
	absRoot := layout.TenantDir(dataDir, tenant)
	roots := layout.AllowedTenantRoots(dataDir, coldDir, tenant)
	var out []metricsFileMeta
	for _, rel := range []string{"hot/current.parquet", "hot/current.duckdb"} {
		p := filepath.Join(absRoot, filepath.FromSlash(rel))
		ok, err := safeTenantParquetInRoots(roots, p)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		minNs, maxNs, bounded := metricsmeta.FileBounds(ctx, p)
		if !bounded {
			continue
		}
		out = append(out, metricsFileMeta{Path: p, MinTsNs: minNs, MaxTsNs: maxNs, hot: true})
	}
	appendTier := func(root string, minTier int) error {
		for tier := minTier; tier < maxTier; tier++ {
			dir := filepath.Join(root, tenant, "tiers", fmt.Sprintf("L%d", tier))
			entries, err := os.ReadDir(dir)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return err
			}
			retired := layout.CompactedSet(entries)
			for _, e := range entries {
				if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
					continue
				}
				ext := filepath.Ext(e.Name())
				if ext != ".parquet" && ext != ".duckdb" {
					continue
				}
				if _, held := retired[e.Name()]; held {
					continue
				}
				p := filepath.Join(dir, e.Name())
				ok, err := safeTenantParquetInRoots(roots, p)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				minNs, maxNs, bounded := metricsmeta.FileBounds(ctx, p)
				if !bounded {
					continue
				}
				out = append(out, metricsFileMeta{Path: p, MinTsNs: minNs, MaxTsNs: maxNs})
			}
		}
		return nil
	}
	if err := appendTier(dataDir, 0); err != nil {
		return nil, err
	}
	if layout.ColdEnabled(coldDir) {
		if err := appendTier(coldDir, 1); err != nil {
			return nil, err
		}
	}
	return out, nil
}
