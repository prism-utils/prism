package merge

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/segformat"
)

const logLandingTier = -1

// logIngestTSColumn is the per-row storage ingest-time axis (nanoseconds) written
// at land/merge so compaction does not collapse charts to merge wall-clock.
const logIngestTSColumn = "__prism_ts_ns"

// LogMergeAction merges log Sources into DestTier under Artifact.
type LogMergeAction struct {
	Artifact string
	Sources  []Segment
	DestTier int
}

// StatLogSegment reads byte size and time bounds from the window id in the filename.
func StatLogSegment(path string, tier int) (Segment, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Segment{}, err
	}
	minTs, maxTs := logSegmentBounds(path, info.ModTime())
	return Segment{
		Tier:  tier,
		Path:  path,
		Bytes: info.Size(),
		MinTs: minTs,
		MaxTs: maxTs,
	}, nil
}

func logSegmentBounds(path string, mtime time.Time) (minTs, maxTs time.Time) {
	if ns, ok := layout.WindowIDNanos(path); ok {
		ts := time.Unix(0, ns).UTC()
		return ts, ts
	}
	ts := mtime.UTC()
	return ts, ts
}

// ScanLogLanding lists landing parquet files for one logs artifact.
func ScanLogLanding(dataDir, tenant, artifact string) ([]Segment, error) {
	dir := layout.LogsLandingDir(dataDir, tenant, artifact)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []Segment
	for _, e := range entries {
		if e.IsDir() || !isSegmentFile(e.Name()) {
			continue
		}
		path := filepath.Join(dir, e.Name())
		seg, err := StatLogSegment(path, logLandingTier)
		if err != nil {
			return nil, err
		}
		out = append(out, seg)
	}
	return out, nil
}

// ScanLogTier lists segments in one logs tier directory.
func ScanLogTier(dataDir, tenant, artifact string, tier int) ([]Segment, error) {
	dir := layout.LogsTierDir(dataDir, tenant, artifact, tier)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []Segment
	for _, e := range entries {
		if e.IsDir() || !isSegmentFile(e.Name()) {
			continue
		}
		path := filepath.Join(dir, e.Name())
		seg, err := StatLogSegment(path, tier)
		if err != nil {
			return nil, err
		}
		out = append(out, seg)
	}
	return out, nil
}

// ScanLogTiers returns segments from L0..maxTier for one logs artifact.
func ScanLogTiers(dataDir, tenant, artifact string, maxTier int) ([]Segment, error) {
	var all []Segment
	for tier := 0; tier <= maxTier; tier++ {
		segs, err := ScanLogTier(dataDir, tenant, artifact, tier)
		if err != nil {
			return nil, err
		}
		all = append(all, segs...)
	}
	return all, nil
}

// FindLogMerges plans log compaction. When both landing and a cold tier are
// eligible, both actions are returned so one tick can drain landing and pack
// L0 toward MaxSegmentBytes (landing must not starve tier catch-up).
func (p *Planner) FindLogMerges(landing, tiers []Segment) []LogMergeAction {
	var out []LogMergeAction
	if action, ok := p.findLogLandingMerge(landing); ok {
		out = append(out, action)
	}
	if action, ok := p.findLogTierPack(tiers); ok {
		out = append(out, action)
	}
	return out
}

func (p *Planner) findLogLandingMerge(landing []Segment) (LogMergeAction, bool) {
	sources, ok := p.packUnsealedLogs(landing)
	if !ok {
		return LogMergeAction{}, false
	}
	return LogMergeAction{Sources: sources, DestTier: 0}, true
}

// findLogTierPack packs the lowest tier with enough unsealed segments toward
// MaxSegmentBytes. Unlike metrics findMergeForTier, it does not require
// Lucene time-adjacency — log L0 files from merge ticks are often minutes
// apart (point windows), so adjacency would never form a pack.
func (p *Planner) findLogTierPack(tiers []Segment) (LogMergeAction, bool) {
	byTier := map[int][]Segment{}
	for _, s := range tiers {
		if s.Bytes >= p.cfg.MaxSegmentBytes {
			continue
		}
		byTier[s.Tier] = append(byTier[s.Tier], s)
	}
	for _, tier := range sortedKeys(byTier) {
		sources, ok := p.packUnsealedLogs(byTier[tier])
		if !ok {
			continue
		}
		return LogMergeAction{Sources: sources, DestTier: tier + 1}, true
	}
	return LogMergeAction{}, false
}

// packUnsealedLogs returns a time-ordered subset of unsealed segments that
// fills toward MaxSegmentBytes (capped by MaxMergeAtOnce), once the
// SegmentsPerTier trigger is met.
func (p *Planner) packUnsealedLogs(segs []Segment) ([]Segment, bool) {
	var live []Segment
	for _, s := range segs {
		if s.Bytes >= p.cfg.MaxSegmentBytes {
			continue
		}
		live = append(live, s)
	}
	if len(live) < p.cfg.SegmentsPerTier {
		return nil, false
	}
	sort.Slice(live, func(i, j int) bool {
		if live[i].MinTs.Equal(live[j].MinTs) {
			return live[i].Path < live[j].Path
		}
		return live[i].MinTs.Before(live[j].MinTs)
	})
	n := p.cfg.MaxMergeAtOnce
	if n > len(live) {
		n = len(live)
	}
	candidates := live[:n]
	for n := len(candidates); n >= 1; n-- {
		subset := candidates[:n]
		sum := int64(0)
		for _, s := range subset {
			sum += s.Bytes
		}
		if sum <= p.cfg.MaxSegmentBytes {
			return subset, true
		}
	}
	return nil, false
}

// ExecuteLogMerge compacts sources into logs/<artifact>/tiers/L{DestTier}/ via union_by_name.
// Per-source rows are stamped with __prism_ts_ns (ingest window ns). The output
// filename uses min source MinTs so legacy filename consumers stay near truth;
// now is only a fallback when bounds are unset.
func (x *Executor) ExecuteLogMerge(artifact string, action LogMergeAction, now time.Time) (Segment, error) {
	if len(action.Sources) == 0 {
		return Segment{}, fmt.Errorf("log merge: no sources")
	}
	destDir := layout.LogsTierDir(x.cfg.DataDir, x.cfg.Tenant, artifact, action.DestTier)
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return Segment{}, err
	}
	minTs, maxTs := mergedLogBounds(action.Sources)
	nameTs := minTs
	if nameTs.IsZero() {
		nameTs = now
	}
	final := filepath.Join(destDir, layout.SegmentNameFormat(nameTs, x.cfg.SegmentFormat.Ext()))
	tmp := final + ".tmp"

	fromParts, cleanup, err := x.sourcesSelectSQLLogs(action.Sources)
	if err != nil {
		return Segment{}, err
	}
	defer cleanup()

	// Per-source arms project __prism_ts_ns (ingest ns); bulk list read cannot.
	union := fromParts[0]
	for _, p := range fromParts[1:] {
		union += " UNION ALL BY NAME " + p
	}
	selectSQL := fmt.Sprintf("SELECT * FROM (%s)", union)

	switch x.cfg.SegmentFormat {
	case segformat.DuckDB:
		if err := segformat.AtomicExportDuckDB(x.db, selectSQL, final, x.cfg.DuckDBStorageVersion, segformat.LogsTable); err != nil {
			return Segment{}, fmt.Errorf("log merge duckdb export: %w", err)
		}
	default:
		// DuckDB read_parquet paths must be literal strings; server-owned paths only.
		//nolint:gosec // G201: parquet paths are server-owned literals; DuckDB cannot bind file paths.
		copySQL := fmt.Sprintf(`
			COPY (%s) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE %d)
		`, selectSQL, layout.ToSlash(tmp), x.cfg.RowGroupSize)
		if _, err := x.db.ExecContext(context.Background(), copySQL); err != nil {
			_ = os.Remove(tmp)
			return Segment{}, fmt.Errorf("log merge copy: %w", err)
		}
		if err := os.Rename(tmp, final); err != nil {
			_ = os.Remove(tmp)
			return Segment{}, err
		}
	}

	info, err := os.Stat(final)
	if err != nil {
		_ = os.Remove(final)
		return Segment{}, err
	}
	seg := Segment{
		Tier:  action.DestTier,
		Path:  final,
		Bytes: info.Size(),
		MinTs: minTs,
		MaxTs: maxTs,
	}
	for _, s := range action.Sources {
		if err := os.Remove(s.Path); err != nil && !os.IsNotExist(err) {
			return Segment{}, fmt.Errorf("log merge: delete %s: %w", s.Path, err)
		}
	}
	return seg, nil
}

func mergedLogBounds(sources []Segment) (minTs, maxTs time.Time) {
	for i, s := range sources {
		if i == 0 || s.MinTs.Before(minTs) {
			minTs = s.MinTs
		}
		if i == 0 || s.MaxTs.After(maxTs) {
			maxTs = s.MaxTs
		}
	}
	return minTs, maxTs
}

// ListLogArtifacts returns logs-* artifact directory names for a tenant.
func ListLogArtifacts(dataDir, tenant string) ([]string, error) {
	logsRoot := filepath.Join(dataDir, tenant, "logs")
	entries, err := os.ReadDir(logsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "logs-") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}
