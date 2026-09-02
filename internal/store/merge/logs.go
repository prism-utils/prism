package merge

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/segformat"
)

const logLandingTier = -1

const (
	// logsPackFloorBytes is the landing pack floor when the planner floor is
	// larger (metrics 24h windows). Tiny landings would otherwise pack only a
	// handful of files per action.
	logsPackFloorBytes int64 = 1 << 20
	// logsPackAtOnceCap bounds files per landing/tier pack so DuckDB merge RAM
	// stays inside the process memory limit.
	logsPackAtOnceCap = 64
)

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
	retired := layout.CompactedSet(entries)
	skipped := layout.MergeSkipSet(entries)
	var out []Segment
	for _, e := range entries {
		if e.IsDir() || !isSegmentFile(e.Name()) {
			continue
		}
		if _, held := retired[e.Name()]; held {
			continue
		}
		if _, skip := skipped[e.Name()]; skip {
			continue
		}
		path := filepath.Join(dir, e.Name())
		seg, err := StatLogSegment(path, logLandingTier)
		if err != nil {
			return nil, err
		}
		if segformat.SkipOpen(path, seg.Bytes) {
			continue
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
	retired := layout.CompactedSet(entries)
	skipped := layout.MergeSkipSet(entries)
	var out []Segment
	for _, e := range entries {
		if e.IsDir() || !isSegmentFile(e.Name()) {
			continue
		}
		if _, held := retired[e.Name()]; held {
			continue
		}
		if _, skip := skipped[e.Name()]; skip {
			continue
		}
		path := filepath.Join(dir, e.Name())
		seg, err := StatLogSegment(path, tier)
		if err != nil {
			return nil, err
		}
		if segformat.SkipOpen(path, seg.Bytes) {
			continue
		}
		out = append(out, seg)
	}
	return out, nil
}

// ScanLogTiers returns segments from L0..maxTier for one logs artifact.
func ScanLogTiers(dataDir, tenant, artifact string, maxTier int) ([]Segment, error) {
	return ScanLogTiersRoots(dataDir, "", tenant, artifact, maxTier)
}

// ScanLogTiersRoots unions hot and cold log tiers including L0.
func ScanLogTiersRoots(dataDir, coldDir, tenant, artifact string, maxTier int) ([]Segment, error) {
	var all []Segment
	for tier := 0; tier <= maxTier; tier++ {
		segs, err := ScanLogTier(dataDir, tenant, artifact, tier)
		if err != nil {
			return nil, err
		}
		all = append(all, segs...)
	}
	if coldDir == "" {
		return all, nil
	}
	for tier := 0; tier <= maxTier; tier++ {
		segs, err := ScanLogTier(coldDir, tenant, artifact, tier)
		if err != nil {
			return nil, err
		}
		all = append(all, segs...)
	}
	return all, nil
}

// FindLogMerges plans log compaction as of now. Landing refreshes are planned
// first and may span several actions, so a searchable-lag backlog drains ahead
// of cold-tier packing; the eligible cold tier is still returned in the same
// pass so landing traffic cannot starve tier catch-up.
func (p *Planner) FindLogMerges(now time.Time, landing, tiers []Segment) []LogMergeAction {
	out := p.findLogLandingRefreshes(now, landing)
	out = append(out, p.findLogTierPacks(now, tiers)...)
	return out
}

// findLogLandingRefreshes packs the live landing buffer into disjoint landing→L0
// actions while a trigger still fires and the action budget holds. When the
// budget is at least two packs and the live set is larger than one pack, the
// first action is newest-first so last-hour queries become searchable before
// the oldest-first drain finishes. Remaining actions stay oldest-first.
func (p *Planner) findLogLandingRefreshes(now time.Time, landing []Segment) []LogMergeAction {
	var out []LogMergeAction
	for _, group := range groupByPayload(landing) {
		if len(out) >= p.cfg.LogsRefreshMaxActions {
			break
		}
		remain := p.cfg.LogsRefreshMaxActions - len(out)
		out = append(out, p.findLogLandingRefreshesOne(now, group, remain)...)
	}
	return out
}

func (p *Planner) findLogLandingRefreshesOne(now time.Time, landing []Segment, maxActions int) []LogMergeAction {
	live := p.sortedLiveLogs(landing)
	var out []LogMergeAction
	if maxActions >= 2 && len(live) > 0 && p.logRefreshDue(now, live) {
		if sources, ok := p.packLiveLogsNewest(live); ok && len(live) > len(sources) {
			out = append(out, LogMergeAction{Sources: sources, DestTier: 0})
			live = subtractLogSources(live, sources)
		}
	}
	for len(out) < maxActions && len(live) > 0 {
		if !p.logRefreshDue(now, live) {
			break
		}
		sources, ok := p.packLiveLogs(live)
		if !ok {
			break
		}
		out = append(out, LogMergeAction{Sources: sources, DestTier: 0})
		live = live[len(sources):]
	}
	return out
}

// logRefreshDue reports whether the buffered segments have either accumulated
// to the count trigger or aged past the refresh interval. The age arm reads the
// oldest segment because that is the row that has been invisible the longest.
func (p *Planner) logRefreshDue(now time.Time, live []Segment) bool {
	if len(live) >= p.cfg.SegmentsPerTier {
		return true
	}
	if p.cfg.LogsRefreshInterval <= 0 {
		return false
	}
	return !live[0].MinTs.After(now.Add(-p.cfg.LogsRefreshInterval))
}

// findLogTierPacks packs the lowest tier with enough unsealed same-type
// segments toward MaxSegmentBytes. Unlike metrics findMergeForTier, it does
// not require Lucene time-adjacency — log L0 files from merge ticks are often
// minutes apart (point windows), so adjacency would never form a pack.
func (p *Planner) findLogTierPacks(now time.Time, tiers []Segment) []LogMergeAction {
	byTier := map[int][]Segment{}
	for _, s := range tiers {
		if s.Bytes >= p.cfg.MaxSegmentBytes {
			continue
		}
		byTier[s.Tier] = append(byTier[s.Tier], s)
	}
	for _, tier := range sortedKeys(byTier) {
		var out []LogMergeAction
		for _, group := range groupByPayload(byTier[tier]) {
			sources, ok := p.packUnsealedLogs(now, group)
			if !ok {
				continue
			}
			out = append(out, LogMergeAction{Sources: sources, DestTier: tier + 1})
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

// packUnsealedLogs returns a time-ordered subset of unsealed segments that
// fills toward MaxSegmentBytes (capped by MaxMergeAtOnce), once the
// SegmentsPerTier trigger is met or ColdAfter ages the set.
func (p *Planner) packUnsealedLogs(now time.Time, segs []Segment) ([]Segment, bool) {
	live := p.sortedLiveLogs(segs)
	if len(live) == 0 {
		return nil, false
	}
	if len(live) < p.cfg.SegmentsPerTier && !p.logsColdDue(now, live) {
		return nil, false
	}
	return p.packLiveLogs(live)
}

func (p *Planner) logsColdDue(now time.Time, live []Segment) bool {
	if p.cfg.ColdAfter <= 0 {
		return false
	}
	cutoff := now.Add(-p.cfg.ColdAfter)
	for _, s := range live {
		if !s.MaxTs.IsZero() && !s.MaxTs.After(cutoff) {
			return true
		}
	}
	return false
}

func groupByPayload(segs []Segment) [][]Segment {
	var order []segformat.Format
	grouped := map[segformat.Format][]Segment{}
	for _, s := range segs {
		f := segmentPayload(s.Path)
		if _, ok := grouped[f]; !ok {
			order = append(order, f)
		}
		grouped[f] = append(grouped[f], s)
	}
	out := make([][]Segment, 0, len(order))
	for _, f := range order {
		out = append(out, grouped[f])
	}
	return out
}

func segmentPayload(path string) segformat.Format {
	if f := segformat.Payload(path); f != "" {
		return f
	}
	if segformat.IsDuckDB(path) {
		return segformat.DuckDB
	}
	return segformat.Parquet
}

func homogeneousPayload(sources []Segment) (segformat.Format, error) {
	if len(sources) == 0 {
		return "", fmt.Errorf("merge: no sources")
	}
	got := segmentPayload(sources[0].Path)
	for _, s := range sources[1:] {
		if segmentPayload(s.Path) != got {
			return "", fmt.Errorf("merge: mixed parquet and duckdb sources")
		}
	}
	return got, nil
}

// sortedLiveLogs drops sealed segments and orders the rest oldest first.
func (p *Planner) sortedLiveLogs(segs []Segment) []Segment {
	live := make([]Segment, 0, len(segs))
	for _, s := range segs {
		if s.Bytes >= p.cfg.MaxSegmentBytes {
			continue
		}
		live = append(live, s)
	}
	sort.Slice(live, func(i, j int) bool {
		if live[i].MinTs.Equal(live[j].MinTs) {
			return live[i].Path < live[j].Path
		}
		return live[i].MinTs.Before(live[j].MinTs)
	})
	return live
}

// logPackAtOnce is how many live log segments one action may attach. When the
// planner floor is above 1 MiB (shared with metrics), logs re-derive from 1 MiB
// and cap at 64 so 536 KiB landings fill toward the seal budget.
func (p *Planner) logPackAtOnce() int {
	n := p.cfg.MaxMergeAtOnce
	if p.cfg.FloorBytes > logsPackFloorBytes {
		n = derivedMaxMergeAtOnce(p.cfg.MaxSegmentBytes, logsPackFloorBytes, p.cfg.SegmentsPerTier)
		if n > logsPackAtOnceCap {
			n = logsPackAtOnceCap
		}
	}
	if n < 1 {
		n = 1
	}
	return n
}

// packLiveLogs fills toward MaxSegmentBytes from the head of a live, time-ordered
// candidate set (capped by the logs pack width), shrinking the set until the
// summed bytes fit the seal budget.
func (p *Planner) packLiveLogs(live []Segment) ([]Segment, bool) {
	return packLiveLogsFrom(live, p.logPackAtOnce(), p.cfg.MaxSegmentBytes, false)
}

// packLiveLogsNewest packs from the tail of a live, oldest-first set so recent
// windows become searchable first. The returned subset stays oldest-first.
func (p *Planner) packLiveLogsNewest(live []Segment) ([]Segment, bool) {
	return packLiveLogsFrom(live, p.logPackAtOnce(), p.cfg.MaxSegmentBytes, true)
}

func packLiveLogsFrom(live []Segment, maxAtOnce int, maxBytes int64, newest bool) ([]Segment, bool) {
	if len(live) == 0 || maxAtOnce < 1 {
		return nil, false
	}
	n := maxAtOnce
	if n > len(live) {
		n = len(live)
	}
	var candidates []Segment
	if newest {
		candidates = live[len(live)-n:]
	} else {
		candidates = live[:n]
	}
	if newest {
		for start := 0; start < len(candidates); start++ {
			subset := candidates[start:]
			if sumSegmentBytes(subset) <= maxBytes {
				return subset, true
			}
		}
		return nil, false
	}
	for n := len(candidates); n >= 1; n-- {
		subset := candidates[:n]
		if sumSegmentBytes(subset) <= maxBytes {
			return subset, true
		}
	}
	return nil, false
}

func sumSegmentBytes(segs []Segment) int64 {
	var sum int64
	for _, s := range segs {
		sum += s.Bytes
	}
	return sum
}

func subtractLogSources(live, drop []Segment) []Segment {
	skip := make(map[string]struct{}, len(drop))
	for _, s := range drop {
		skip[s.Path] = struct{}{}
	}
	out := make([]Segment, 0, len(live)-len(drop))
	for _, s := range live {
		if _, hit := skip[s.Path]; hit {
			continue
		}
		out = append(out, s)
	}
	return out
}

// ExecuteLogMerge writes packed log sources into logs/<artifact>/tiers/L{DestTier}/.
// Dest extension follows source payload: duckdb sources stay duckdb even when
// MERGE_SEGMENT_FORMAT is parquet. Mixed parquet+duckdb lists error. Per-source
// rows are stamped with __prism_ts_ns (ingest window ns). The output filename
// uses min source MinTs so legacy filename consumers stay near truth; now is
// only a fallback when bounds are unset.
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
	srcFmt, err := homogeneousPayload(action.Sources)
	if err != nil {
		return Segment{}, err
	}
	destFmt := x.cfg.SegmentFormat
	if srcFmt == segformat.DuckDB {
		destFmt = segformat.DuckDB
	}
	final := filepath.Join(destDir, layout.SegmentNameFormat(nameTs, destFmt.Ext()))

	switch destFmt {
	case segformat.DuckDB:
		if err := x.mergeLogsDuckDB(action.Sources, final); err != nil {
			return Segment{}, err
		}
	default:
		var kerr error
		if x.cfg.FailKway {
			kerr = fmt.Errorf("log merge k-way: forced failure")
		} else {
			kerr = kwayMergeLogs(final, action.Sources, nil)
		}
		if kerr != nil {
			_ = os.Remove(final)
			if err := x.mergeLogsCopy(action.Sources, final); err != nil {
				return Segment{}, err
			}
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
	if err := retireSources(action.Sources, now, x.cfg.DeleteGrace); err != nil {
		return Segment{}, err
	}
	return seg, nil
}

func (x *Executor) mergeLogsDuckDB(sources []Segment, final string) error {
	fromParts, cleanup, err := x.sourcesSelectSQLLogs(sources)
	if err != nil {
		return err
	}
	defer cleanup()
	union := fromParts[0]
	for _, p := range fromParts[1:] {
		union += " UNION ALL BY NAME " + p
	}
	selectSQL := fmt.Sprintf("SELECT * FROM (%s) ORDER BY %s", union, logIngestTSColumn)
	if err := segformat.AtomicExportDuckDB(x.db, selectSQL, final, x.cfg.DuckDBStorageVersion, segformat.LogsTable); err != nil {
		return fmt.Errorf("log merge duckdb export: %w", err)
	}
	return nil
}

func (x *Executor) mergeLogsCopy(sources []Segment, final string) error {
	if x.cfg.FailCopy {
		return fmt.Errorf("log merge copy: forced failure")
	}
	tmp := final + ".tmp"
	fromParts, cleanup, err := x.sourcesSelectSQLLogs(sources)
	if err != nil {
		return err
	}
	defer cleanup()
	union := fromParts[0]
	for _, p := range fromParts[1:] {
		union += " UNION ALL BY NAME " + p
	}
	selectSQL := fmt.Sprintf("SELECT * FROM (%s) ORDER BY %s", union, logIngestTSColumn)
	//nolint:gosec // G201: parquet paths are server-owned literals; DuckDB cannot bind file paths.
	copySQL := fmt.Sprintf(`
			COPY (%s) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE %d)
		`, selectSQL, layout.ToSlash(tmp), x.cfg.RowGroupSize)
	if _, err := x.db.ExecContext(context.Background(), copySQL); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("log merge copy: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	x.CopyCount++
	return nil
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
