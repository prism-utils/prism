package query

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/logmeta"
)

// logFileMeta is one landed or compacted log segment the planners may open.
type logFileMeta struct {
	Path        string
	Artifact    string
	Bytes       int64
	MinTsNs     int64
	MaxTsNs     int64
	Mtime       time.Time
	HasIngestTS bool   // file already carries per-row __prism_ts_ns
	duckAlias   string // set after sandbox ATTACH for .duckdb segments
}

// logsCatalogOpts controls how the shared logs relation is built.
type logsCatalogOpts struct {
	// StartNs/EndNs, when EndNs > StartNs, keep only files overlapping [Start,End).
	StartNs int64
	EndNs   int64
	// WithIngestTS adds __prism_ts_ns (Loki time axis). Prefers a per-row column
	// when present; falls back to a path→ts filename JOIN for legacy files.
	WithIngestTS bool
	// OmitMessage drops the message column from the relation (label APIs).
	OmitMessage bool
	// RecentOnly caps the open set to files with MaxTsNs >= now-lookback when
	// the request did not already narrow the range (End-Start covers lookback).
	RecentOnly     bool
	RecentLookback time.Duration
	Now            time.Time
}

// logsFileMetaCache is a process-local cache of log file metadata per tenant root.
type logsFileMetaCache struct {
	mu     sync.Mutex
	byRoot map[string]logsCacheEntry
	// rescans counts full directory walks (tests assert the second labels call
	// does not increment this when the tree is unchanged).
	rescans int
}

type logsCacheEntry struct {
	files   []logFileMeta
	version uint64
}

var globalLogsMetaCache = &logsFileMetaCache{byRoot: map[string]logsCacheEntry{}}

// InvalidateLogsMetaCache drops cached metadata for one tenant root (or all when
// empty). Land/merge/retention call this so planners see fresh files.
func InvalidateLogsMetaCache(absTenantRoot string) {
	globalLogsMetaCache.mu.Lock()
	defer globalLogsMetaCache.mu.Unlock()
	if absTenantRoot == "" {
		globalLogsMetaCache.byRoot = map[string]logsCacheEntry{}
		return
	}
	root := filepath.Clean(absTenantRoot)
	if abs, err := filepath.Abs(root); err == nil {
		root = abs
		if resolved, err := filepath.EvalSymlinks(root); err == nil {
			root = resolved
		}
	}
	delete(globalLogsMetaCache.byRoot, root)
	delete(globalLogsMetaCache.byRoot, filepath.Clean(absTenantRoot))
}

func (c *logsFileMetaCache) getOrScan(absTenantRoot string) ([]logFileMeta, error) {
	root := filepath.Clean(absTenantRoot)
	gen, err := logmeta.Read(filepath.Dir(root), filepath.Base(root))
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if e, ok := c.byRoot[root]; ok && e.version == gen {
		out := append([]logFileMeta(nil), e.files...)
		c.mu.Unlock()
		return out, nil
	}
	c.mu.Unlock()

	files, err := scanLogParquetFiles(root)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.rescans++
	c.byRoot[root] = logsCacheEntry{files: files, version: gen}
	out := append([]logFileMeta(nil), files...)
	c.mu.Unlock()
	return out, nil
}

func (c *logsFileMetaCache) rescanCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rescans
}

// scanLogParquetFiles walks landing windows and compacted tiers under
// <tenant>/logs/<artifact>/ and <tenant>/logs/<artifact>/tiers/L{n}/.
// When manifests match the generation stamp they are preferred over directory walks.
func scanLogParquetFiles(absTenantRoot string) ([]logFileMeta, error) {
	dataDir := filepath.Dir(absTenantRoot)
	tenant := filepath.Base(absTenantRoot)
	gen, err := logmeta.Read(dataDir, tenant)
	if err != nil {
		return nil, err
	}
	logsRoot := filepath.Join(absTenantRoot, "logs")
	entries, err := os.ReadDir(logsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []logFileMeta
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "logs-") {
			continue
		}
		artifact := e.Name()
		files, err := scanArtifactLogFiles(absTenantRoot, dataDir, tenant, artifact, gen)
		if err != nil {
			return nil, err
		}
		out = append(out, files...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func scanArtifactLogFiles(absTenantRoot, dataDir, tenant, artifact string, gen uint64) ([]logFileMeta, error) {
	mpath := logmeta.ManifestPath(dataDir, tenant, artifact)
	if _, err := os.Stat(mpath); err == nil {
		m, err := logmeta.ReadManifest(dataDir, tenant, artifact)
		if err == nil && m.Version == gen {
			if files, ok := manifestToLogFiles(absTenantRoot, dataDir, tenant, artifact, m); ok {
				return files, nil
			}
		}
	}
	// Missing, stale, or corrupt manifest: rebuild listing from disk.
	var out []logFileMeta
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	files, err := listParquetInDir(absTenantRoot, landing, artifact)
	if err != nil {
		return nil, err
	}
	out = append(out, files...)
	tiersRoot := filepath.Join(landing, "tiers")
	tierEntries, err := os.ReadDir(tiersRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	for _, te := range tierEntries {
		if !te.IsDir() || !strings.HasPrefix(te.Name(), "L") {
			continue
		}
		tierFiles, err := listParquetInDir(absTenantRoot, filepath.Join(tiersRoot, te.Name()), artifact)
		if err != nil {
			return nil, err
		}
		out = append(out, tierFiles...)
	}
	return out, nil
}

func manifestToLogFiles(absTenantRoot, dataDir, tenant, artifact string, m logmeta.Manifest) ([]logFileMeta, bool) {
	if len(m.Files) == 0 {
		return nil, true
	}
	artifactRoot := layout.LogsLandingDir(dataDir, tenant, artifact)
	out := make([]logFileMeta, 0, len(m.Files))
	for _, f := range m.Files {
		abs := filepath.Join(artifactRoot, filepath.FromSlash(f.Path))
		ok, err := safeTenantParquetFile(absTenantRoot, abs)
		if err != nil || !ok {
			return nil, false
		}
		fi, err := os.Stat(abs) //nolint:gosec // G703: path validated inside tenant root
		if err != nil {
			if os.IsNotExist(err) {
				return nil, false
			}
			return nil, false
		}
		minNs, maxNs := f.MinTsNs, f.MaxTsNs
		if minNs == 0 && maxNs == 0 {
			minNs, maxNs = logFileTimeBounds(abs, fi.ModTime())
		}
		out = append(out, logFileMeta{
			Path:        abs,
			Artifact:    artifact,
			Bytes:       f.Bytes,
			MinTsNs:     minNs,
			MaxTsNs:     maxNs,
			Mtime:       fi.ModTime().UTC(),
			HasIngestTS: logSegmentHasIngestTS(abs),
		})
	}
	return out, true
}

func listParquetInDir(absTenantRoot, dir, artifact string) ([]logFileMeta, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []logFileMeta
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		ext := filepath.Ext(e.Name())
		if ext != ".parquet" && ext != ".duckdb" {
			continue
		}
		match := filepath.Join(dir, e.Name())
		ok, err := safeTenantParquetFile(absTenantRoot, match)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		fi, err := os.Stat(match) //nolint:gosec // G703: path validated inside tenant root
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		minNs, maxNs := logFileTimeBounds(match, fi.ModTime())
		out = append(out, logFileMeta{
			Path:        match,
			Artifact:    artifact,
			Bytes:       fi.Size(),
			MinTsNs:     minNs,
			MaxTsNs:     maxNs,
			Mtime:       fi.ModTime().UTC(),
			HasIngestTS: logSegmentHasIngestTS(match),
		})
	}
	return out, nil
}

// logFileTimeBounds returns [min,max] ingest-time nanoseconds for a window.
// Prefers the leading unix_ns from layout.SegmentName; falls back to mtime.
func logFileTimeBounds(path string, mtime time.Time) (minNs, maxNs int64) {
	if ns, ok := layout.WindowIDNanos(path); ok {
		return ns, ns
	}
	ns := mtime.UTC().UnixNano()
	return ns, ns
}

func filterLogFiles(files []logFileMeta, opts logsCatalogOpts) []logFileMeta {
	start, end := opts.StartNs, opts.EndNs
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if opts.RecentOnly && opts.RecentLookback > 0 {
		floor := now.Add(-opts.RecentLookback).UnixNano()
		if start == 0 || start < floor {
			start = floor
		}
		if end <= start {
			end = now.UnixNano()
		}
	}
	if end <= start {
		return append([]logFileMeta(nil), files...)
	}
	out := make([]logFileMeta, 0, len(files))
	for _, f := range files {
		if f.MaxTsNs < start || f.MinTsNs >= end {
			continue
		}
		out = append(out, f)
	}
	return out
}

// buildLogsRelationSQL builds the shared logs relation body over the given files.
// Depth is O(1) in file count (one read_parquet list per ingest-ts group), matching /sql.
func buildLogsRelationSQL(files []logFileMeta, opts logsCatalogOpts) string {
	if len(files) == 0 {
		if opts.WithIngestTS {
			return emptyLokiLogsViewSQL
		}
		return emptyLogsViewSQL
	}
	if !opts.WithIngestTS {
		quoted := make([]string, len(files))
		for i, f := range files {
			quoted[i] = "'" + escapeSQLLiteral(layout.ToSlash(f.Path)) + "'"
		}
		base := fmt.Sprintf("read_parquet([%s], union_by_name=true, filename=true)", strings.Join(quoted, ", "))
		inner := fmt.Sprintf(`SELECT * EXCLUDE (filename) FROM (SELECT * FROM %s)`, base)
		if opts.OmitMessage {
			return fmt.Sprintf(`SELECT * EXCLUDE (%s) FROM (%s)`, lokiMessageColumn, inner)
		}
		return inner
	}

	var withCol, legacy []logFileMeta
	for _, f := range files {
		if f.HasIngestTS {
			withCol = append(withCol, f)
		} else {
			legacy = append(legacy, f)
		}
	}
	var parts []string
	if len(withCol) > 0 {
		parts = append(parts, buildLogsIngestTSColumnSQL(withCol, opts.OmitMessage))
	}
	if len(legacy) > 0 {
		parts = append(parts, buildLogsIngestTSFilenameSQL(legacy, opts.OmitMessage))
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts, " UNION ALL BY NAME ")
}

// buildLogsIngestTSColumnSQL projects the per-row __prism_ts_ns already stored in files.
func buildLogsIngestTSColumnSQL(files []logFileMeta, omitMessage bool) string {
	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = "'" + escapeSQLLiteral(layout.ToSlash(f.Path)) + "'"
	}
	base := fmt.Sprintf("read_parquet([%s], union_by_name=true)", strings.Join(quoted, ", "))
	// Name the column explicitly so planners and tests see the Loki time axis in SQL
	// (SELECT * alone would carry it silently from the parquet schema).
	inner := fmt.Sprintf(
		`SELECT t.* EXCLUDE (%s), t.%s AS %s FROM %s AS t`,
		lokiTSColumn, lokiTSColumn, lokiTSColumn, base,
	)
	if omitMessage {
		return fmt.Sprintf(`SELECT * EXCLUDE (%s) FROM (%s)`, lokiMessageColumn, inner)
	}
	return inner
}

// buildLogsIngestTSFilenameSQL stamps __prism_ts_ns from each file's window id (legacy).
func buildLogsIngestTSFilenameSQL(files []logFileMeta, omitMessage bool) string {
	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = "'" + escapeSQLLiteral(layout.ToSlash(f.Path)) + "'"
	}
	base := fmt.Sprintf("read_parquet([%s], union_by_name=true, filename=true)", strings.Join(quoted, ", "))
	inner := "SELECT * FROM " + base
	values := make([]string, len(files))
	for i, f := range files {
		values[i] = fmt.Sprintf("('%s', %d::BIGINT)",
			escapeSQLLiteral(layout.ToSlash(f.Path)), f.MinTsNs)
	}
	inner = fmt.Sprintf(
		`SELECT l.* EXCLUDE (filename), v.ts AS %s FROM (%s) AS l JOIN (VALUES %s) AS v(path, ts) ON l.filename = v.path`,
		lokiTSColumn, inner, strings.Join(values, ", "),
	)
	if omitMessage {
		return fmt.Sprintf(`SELECT * EXCLUDE (%s) FROM (%s)`, lokiMessageColumn, inner)
	}
	return inner
}

// sandboxLogsRelationSQL is the single builder used by /sql and Loki.
func sandboxLogsRelationSQL(tenantRoot string, opts logsCatalogOpts) (string, []logFileMeta, error) {
	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		return "", nil, err
	}
	if resolved, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = resolved
	}
	absRoot = filepath.Clean(absRoot)

	files, err := globalLogsMetaCache.getOrScan(absRoot)
	if err != nil {
		return "", nil, err
	}
	files = filterLogFiles(files, opts)
	files = filterExistingLogFiles(files)
	hasDuck := false
	for _, f := range files {
		if filepath.Ext(f.Path) == ".duckdb" {
			hasDuck = true
			break
		}
	}
	if hasDuck {
		// .duckdb paths cannot be opened via read_parquet; return the file list
		// with empty SQL so ATTACH aliases can be assigned before a mixed union.
		return "", files, nil
	}
	return buildLogsRelationSQL(files, opts), files, nil
}

// filterExistingLogFiles drops paths that disappeared after the meta cache /
// manifest snapshot (retention, compaction, or a lost rename). DuckDB's
// read_parquet([…]) fails the whole relation if any listed file is missing.
func filterExistingLogFiles(files []logFileMeta) []logFileMeta {
	if len(files) == 0 {
		return files
	}
	out := make([]logFileMeta, 0, len(files))
	for _, f := range files {
		if _, err := os.Stat(f.Path); err != nil { //nolint:gosec // G703: path already tenant-validated
			continue
		}
		out = append(out, f)
	}
	return out
}
