package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

// metricNameLabel is the reserved Prometheus label holding a metric's name.
const metricNameLabel = "__name__"

// errTooManySamples bounds the adapter the same way the engine bounds itself, so
// a pathological selector cannot materialize more rows than MaxSamples before
// the engine's own counter trips. Kept identical in spirit to Prometheus'
// "query processing would load too many samples into memory" guard.
var errTooManySamples = errors.New("query: promql: too many samples")

// sandboxQueryable adapts the per-request DuckDB sandbox metrics view to the
// Prometheus storage.Queryable contract. It never owns the connection: the HTTP
// handler opens and closes the sandbox around the whole query.
type sandboxQueryable struct {
	conn       *sql.Conn
	view       string
	maxSamples int
}

func (q *sandboxQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &sandboxQuerier{conn: q.conn, view: q.view, mint: mint, maxt: maxt, maxSamples: q.maxSamples}, nil
}

type sandboxQuerier struct {
	conn       *sql.Conn
	view       string
	mint, maxt int64
	maxSamples int
	// open tracks streaming cursors so Close releases them if the engine stops
	// consuming a SeriesSet early; the per-request conn is closed regardless.
	open []*sql.Rows
}

func (qr *sandboxQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	mint, maxt := qr.mint, qr.maxt
	if hints != nil {
		if hints.Start != 0 {
			mint = hints.Start
		}
		if hints.End != 0 {
			maxt = hints.End
		}
	}
	sqlText, args := buildSelectSQL(qr.view, mint, maxt, matchers)
	rows, err := qr.conn.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	// When the caller does not need sorted series (e.g. /series), stream series
	// straight off the DuckDB cursor without materializing the whole result.
	if !sortSeries {
		qr.open = append(qr.open, rows)
		return newStreamingSeriesSet(rows, matchers, qr.maxSamples)
	}
	// Sorted consumers need the full set to sort by label order, which the raw
	// `labels` text ordering does not guarantee; materialize then sort.
	defer func() { _ = rows.Close() }()
	series, err := scanSeries(rows, matchers, qr.maxSamples)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].Labels(), series[j].Labels()) < 0
	})
	return &sliceSeriesSet{series: series}
}

func (qr *sandboxQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := labelHintLimit(hints)
	seen := map[string]struct{}{}
	err := qr.forEachDistinctSeries(ctx, matchers, func(lbls labels.Labels) bool {
		// Range by name so a present-but-empty value is kept, which lbls.Get
		// cannot distinguish from an absent label.
		lbls.Range(func(l labels.Label) {
			if l.Name == name {
				seen[l.Value] = struct{}{}
			}
		})
		return limit <= 0 || len(seen) < limit
	})
	if err != nil {
		return nil, nil, err
	}
	return capSorted(seen, limit), nil, nil
}

func (qr *sandboxQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := labelHintLimit(hints)
	seen := map[string]struct{}{}
	err := qr.forEachDistinctSeries(ctx, matchers, func(lbls labels.Labels) bool {
		lbls.Range(func(l labels.Label) { seen[l.Name] = struct{}{} })
		return limit <= 0 || len(seen) < limit
	})
	if err != nil {
		return nil, nil, err
	}
	return capSorted(seen, limit), nil, nil
}

// seriesLabels returns the matching series' label sets, reading only distinct
// (name, labels) pairs — never per-sample rows — so /series stays bounded by
// series cardinality rather than sample count. limit <= 0 means no cap.
func (qr *sandboxQuerier) seriesLabels(ctx context.Context, matchers []*labels.Matcher, limit int) ([]labels.Labels, error) {
	var out []labels.Labels
	err := qr.forEachDistinctSeries(ctx, matchers, func(lbls labels.Labels) bool {
		out = append(out, lbls)
		return limit <= 0 || len(out) < limit
	})
	return out, err
}

func (qr *sandboxQuerier) Close() error {
	for _, r := range qr.open {
		_ = r.Close()
	}
	qr.open = nil
	return nil
}

// forEachDistinctSeries streams the distinct (name, labels) pairs in range,
// applies the matchers in Go, and invokes fn per matching series until fn returns
// false. It reads only distinct label sets (not every sample), so label and
// series endpoints stay bounded by series cardinality rather than sample count.
func (qr *sandboxQuerier) forEachDistinctSeries(ctx context.Context, matchers []*labels.Matcher, fn func(labels.Labels) bool) error {
	//nolint:gosec // G201: view is a package const; time bounds are bound as ? params.
	sqlText := fmt.Sprintf(`SELECT DISTINCT "__name__", labels FROM %s WHERE epoch_ms(ts) >= ? AND epoch_ms(ts) <= ?`, qr.view)
	rows, err := qr.conn.QueryContext(ctx, sqlText, qr.mint, qr.maxt)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, lbl string
		if err := rows.Scan(&name, &lbl); err != nil {
			return err
		}
		lbls, perr := parseSeriesLabels(name, lbl)
		if perr != nil {
			return perr
		}
		if matchesAll(lbls, matchers) {
			if !fn(lbls) {
				return nil
			}
		}
	}
	return rows.Err()
}

// labelHintLimit extracts a non-negative result cap from label hints, or 0 (no
// cap) when hints are absent.
func labelHintLimit(hints *storage.LabelHints) int {
	if hints == nil || hints.Limit <= 0 {
		return 0
	}
	return hints.Limit
}

// capSorted returns the sorted keys, truncated to limit when limit > 0.
func capSorted(m map[string]struct{}, limit int) []string {
	out := sortedKeys(m)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// buildSelectSQL pushes the selective __name__ equality (when present) and the
// time bounds into DuckDB and orders by (name, labels, t) so a single streamed
// cursor groups consecutive rows into series without a server-side sort here.
func buildSelectSQL(view string, mint, maxt int64, matchers []*labels.Matcher) (string, []any) {
	var sb strings.Builder
	//nolint:gosec // G201: view is a package const; all values are bound as ? params.
	fmt.Fprintf(&sb, `SELECT "__name__", labels, value, CAST(epoch_ms(ts) AS BIGINT) AS t FROM %s WHERE epoch_ms(ts) >= ? AND epoch_ms(ts) <= ?`, view)
	args := []any{mint, maxt}
	if name, ok := equalNameMatcher(matchers); ok {
		sb.WriteString(` AND "__name__" = ?`)
		args = append(args, name)
	}
	sb.WriteString(` ORDER BY "__name__", labels, t`)
	return sb.String(), args
}

// equalNameMatcher returns the __name__ value when exactly one equality matcher
// pins the metric name, letting DuckDB prune to a single metric before scanning.
func equalNameMatcher(matchers []*labels.Matcher) (string, bool) {
	for _, m := range matchers {
		if m.Name == metricNameLabel && m.Type == labels.MatchEqual {
			return m.Value, true
		}
	}
	return "", false
}

func scanSeries(rows *sql.Rows, matchers []*labels.Matcher, maxSamples int) ([]storage.Series, error) {
	var out []storage.Series
	var curName, curLbl string
	var curLabels labels.Labels
	var curSamples []chunks.Sample
	keep := false
	first := true
	total := 0

	flush := func() {
		if keep && len(curSamples) > 0 {
			out = append(out, storage.NewListSeries(curLabels, curSamples))
		}
		curSamples = nil
	}

	for rows.Next() {
		var name, lbl string
		var val float64
		var t int64
		if err := rows.Scan(&name, &lbl, &val, &t); err != nil {
			return nil, err
		}
		if first || name != curName || lbl != curLbl {
			if !first {
				flush()
			}
			first = false
			curName, curLbl = name, lbl
			lbls, err := parseSeriesLabels(name, lbl)
			if err != nil {
				return nil, err
			}
			curLabels = lbls
			keep = matchesAll(lbls, matchers)
		}
		if keep {
			total++
			if maxSamples > 0 && total > maxSamples {
				return nil, errTooManySamples
			}
			curSamples = append(curSamples, floatSample{t: t, v: val})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !first {
		flush()
	}
	return out, nil
}

// parseSeriesLabels turns a stored metric name plus the verbatim label text
// between braces (e.g. `a="1",b="2"`) into a sorted Prometheus label set.
func parseSeriesLabels(name, text string) (labels.Labels, error) {
	b := labels.NewScratchBuilder(2)
	b.Add(metricNameLabel, name)
	if err := forEachLabelPair(text, func(k, v string) { b.Add(k, v) }); err != nil {
		return labels.EmptyLabels(), err
	}
	b.Sort()
	return b.Labels(), nil
}

// forEachLabelPair parses `name="value"` pairs separated by commas, honoring
// quoted values with `\"`, `\\`, and `\n` escapes. Whitespace between tokens is
// tolerated. An empty string yields no pairs.
func forEachLabelPair(s string, emit func(name, value string)) error {
	i := 0
	n := len(s)
	skipSpace := func() {
		for i < n && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
	}
	for {
		skipSpace()
		if i >= n {
			return nil
		}
		start := i
		for i < n && s[i] != '=' && s[i] != ',' && s[i] != ' ' && s[i] != '\t' {
			i++
		}
		key := s[start:i]
		skipSpace()
		if i >= n || s[i] != '=' {
			return fmt.Errorf("query: promql: malformed label pair near %q", s[start:])
		}
		i++ // consume '='
		skipSpace()
		if i >= n || s[i] != '"' {
			return fmt.Errorf("query: promql: label %q value not quoted", key)
		}
		i++ // consume opening quote
		var val strings.Builder
		for i < n {
			c := s[i]
			if c == '\\' && i+1 < n {
				i++
				switch s[i] {
				case 'n':
					val.WriteByte('\n')
				case 't':
					val.WriteByte('\t')
				default:
					val.WriteByte(s[i])
				}
				i++
				continue
			}
			if c == '"' {
				break
			}
			val.WriteByte(c)
			i++
		}
		if i >= n || s[i] != '"' {
			return fmt.Errorf("query: promql: unterminated value for label %q", key)
		}
		i++ // consume closing quote
		if key == "" {
			return fmt.Errorf("query: promql: empty label name")
		}
		emit(key, val.String())
		skipSpace()
		if i < n && s[i] == ',' {
			i++
		}
	}
}

func matchesAll(lbls labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lbls.Get(m.Name)) {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sliceSeriesSet is a minimal storage.SeriesSet over a pre-collected slice.
type sliceSeriesSet struct {
	series []storage.Series
	idx    int
}

func (s *sliceSeriesSet) Next() bool {
	if s.idx >= len(s.series) {
		return false
	}
	s.idx++
	return true
}
func (s *sliceSeriesSet) At() storage.Series                { return s.series[s.idx-1] }
func (s *sliceSeriesSet) Err() error                        { return nil }
func (s *sliceSeriesSet) Warnings() annotations.Annotations { return nil }

// streamingSeriesSet groups the sorted DuckDB cursor into series lazily: each
// Next reads only the rows of one series, so peak memory is one series' samples
// rather than the whole result set. It relies on buildSelectSQL ordering rows by
// (name, labels, t) and enforces maxSamples across the full scan like scanSeries.
type streamingSeriesSet struct {
	rows       *sql.Rows
	matchers   []*labels.Matcher
	maxSamples int
	total      int

	// One-row lookahead so a series boundary is detected without unreading.
	haveNext bool
	nName    string
	nLbl     string
	nVal     float64
	nT       int64

	cur  storage.Series
	err  error
	done bool
}

func newStreamingSeriesSet(rows *sql.Rows, matchers []*labels.Matcher, maxSamples int) *streamingSeriesSet {
	s := &streamingSeriesSet{rows: rows, matchers: matchers, maxSamples: maxSamples}
	s.advance()
	return s
}

// advance loads the next cursor row into the lookahead buffer.
func (s *streamingSeriesSet) advance() {
	if s.err != nil {
		s.haveNext = false
		return
	}
	if !s.rows.Next() {
		s.err = s.rows.Err()
		s.haveNext = false
		return
	}
	if err := s.rows.Scan(&s.nName, &s.nLbl, &s.nVal, &s.nT); err != nil {
		s.err = err
		s.haveNext = false
		return
	}
	s.haveNext = true
}

func (s *streamingSeriesSet) Next() bool {
	if s.done || s.err != nil {
		return false
	}
	for s.haveNext {
		name, lbl := s.nName, s.nLbl
		lbls, perr := parseSeriesLabels(name, lbl)
		if perr != nil {
			s.err = perr
			return false
		}
		keep := matchesAll(lbls, s.matchers)
		var samples []chunks.Sample
		for s.haveNext && s.nName == name && s.nLbl == lbl {
			if keep {
				s.total++
				if s.maxSamples > 0 && s.total > s.maxSamples {
					s.err = errTooManySamples
					return false
				}
				samples = append(samples, floatSample{t: s.nT, v: s.nVal})
			}
			s.advance()
			if s.err != nil {
				return false
			}
		}
		if keep && len(samples) > 0 {
			s.cur = storage.NewListSeries(lbls, samples)
			return true
		}
	}
	s.done = true
	return false
}

func (s *streamingSeriesSet) At() storage.Series                { return s.cur }
func (s *streamingSeriesSet) Err() error                        { return s.err }
func (s *streamingSeriesSet) Warnings() annotations.Annotations { return nil }

// floatSample is a float-only chunks.Sample; prism stores gauge/counter floats,
// never native histograms, so the histogram accessors are nil by construction.
type floatSample struct {
	t int64
	v float64
}

func (s floatSample) T() int64                      { return s.t }
func (s floatSample) ST() int64                     { return 0 }
func (s floatSample) F() float64                    { return s.v }
func (s floatSample) H() *histogram.Histogram       { return nil }
func (s floatSample) FH() *histogram.FloatHistogram { return nil }
func (s floatSample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s floatSample) Copy() chunks.Sample           { return s }
