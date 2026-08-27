package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prism-utils/prism/internal/store/httperr"
	storeingest "github.com/prism-utils/prism/internal/store/ingest"
	"github.com/prism-utils/prism/internal/store/logmeta"
	"github.com/prism-utils/prism/internal/store/metrics"
	storetenant "github.com/prism-utils/prism/internal/store/tenant"
)

// Synthetic stream label every log line carries. Grafana's Loki datasource wants
// a non-empty stream selector, and `job` is the conventional entry point, so a
// stock `{job="prism"}` query works against a fresh store.
const (
	lokiJobLabel = "job"
	lokiJobValue = "prism"
)

// Columns with a fixed meaning in a stream: the message is the log line (never a
// label), and the count of a summarized template is exposed as a label string.
const (
	lokiMessageColumn = "message"
	lokiCountColumn   = "count"
)

// Query direction values, matching the Loki API parameter.
const (
	lokiDirectionBackward = "backward"
	lokiDirectionForward  = "forward"
)

// lokiResponse is the Loki HTTP API JSON envelope.
type lokiResponse struct {
	Status string `json:"status"`
	Data   any    `json:"data,omitempty"`
	Error  string `json:"error,omitempty"`
}

// lokiStreamsData is the `data` object of a successful query_range.
type lokiStreamsData struct {
	ResultType string           `json:"resultType"`
	Result     []lokiStreamJSON `json:"result"`
	Stats      map[string]any   `json:"stats"`
}

// lokiStreamJSON is one stream: its label set plus [ns timestamp, line] pairs,
// both encoded as strings the way the Loki API specifies.
type lokiStreamJSON struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

// lokiError carries an HTTP status plus the message returned in the envelope.
type lokiError struct {
	status int
	msg    string
	cause  error
}

// lokiHandler serves the Loki-compatible logs read API for one store.
type lokiHandler struct {
	cfg    *LokiConfig
	logger *slog.Logger
}

// LokiHandler returns an http.Handler serving the Loki API surface (query_range,
// labels, label/<name>/values) over the tenant's landed log Parquet. One handler
// serves every route via a single dispatcher so RBAC, cluster routing, and the
// shared query queue can wrap it uniformly. Logs are file-backed, so no engine or
// hot snapshot is involved.
func LokiHandler(cfg *LokiConfig, logger *slog.Logger) http.Handler {
	if cfg == nil {
		cfg = &LokiConfig{}
	}
	c := *cfg
	c.withDefaults()
	// withDefaults fills every bound, so Validate is defensive; log rather than
	// fail so a store never silently runs an out-of-range limit.
	if err := c.Validate(); err != nil && logger != nil {
		logger.Warn("loki config invalid after defaults", "err", err)
	}
	h := &lokiHandler{cfg: &c, logger: logger}
	return http.HandlerFunc(h.serve)
}

func (h *lokiHandler) serve(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case strings.HasSuffix(path, "/loki/api/v1/query_range"):
		h.handleQueryRange(w, r)
	case strings.HasSuffix(path, "/loki/api/v1/labels"):
		h.handleLabelNames(w, r)
	case strings.Contains(path, "/loki/api/v1/label/") && strings.HasSuffix(path, "/values"):
		h.handleLabelValues(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *lokiHandler) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(w, r); err != nil {
		h.writeError(w, r, &lokiError{status: http.StatusBadRequest, msg: "invalid request body"})
		return
	}
	q, apiErr := parseLokiSelector(r.Form.Get("query"))
	if apiErr != nil {
		h.writeError(w, r, apiErr)
		return
	}
	startNs, endNs, apiErr := lokiQueryRange(r)
	if apiErr != nil {
		h.writeError(w, r, apiErr)
		return
	}
	limit, apiErr := parseLokiLimit(r.Form.Get("limit"), h.cfg.MaxEntries)
	if apiErr != nil {
		h.writeError(w, r, apiErr)
		return
	}
	direction := strings.TrimSpace(r.Form.Get("direction"))
	switch direction {
	case "":
		direction = lokiDirectionBackward
	case lokiDirectionBackward, lokiDirectionForward:
	default:
		h.writeError(w, r, &lokiError{status: http.StatusBadRequest, msg: "invalid direction: want backward or forward"})
		return
	}

	h.withSandbox(w, r, startNs, endNs, false, func(ctx context.Context, rel *lokiRelation) (any, *lokiError) {
		data := lokiStreamsData{ResultType: "streams", Result: []lokiStreamJSON{}, Stats: map[string]any{}}
		pred := rel.predicates(q, startNs, endNs)
		if pred.matchNothing {
			return data, nil
		}
		streams, err := rel.queryStreams(ctx, pred.where, direction == lokiDirectionBackward, limit)
		if err != nil {
			return nil, h.execError(err)
		}
		data.Result = streams
		return data, nil
	})
}

func (h *lokiHandler) handleLabelNames(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(w, r); err != nil {
		h.writeError(w, r, &lokiError{status: http.StatusBadRequest, msg: "invalid request body"})
		return
	}
	q, apiErr := parseLokiSelector(r.Form.Get("query"))
	if apiErr != nil {
		h.writeError(w, r, apiErr)
		return
	}
	startNs, endNs, apiErr := lokiMetadataRange(r)
	if apiErr != nil {
		h.writeError(w, r, apiErr)
		return
	}
	h.withSandbox(w, r, startNs, endNs, true, func(ctx context.Context, rel *lokiRelation) (any, *lokiError) {
		pred := rel.predicates(q, startNs, endNs)
		if pred.matchNothing {
			return []string{}, nil
		}
		names, err := rel.labelNames(ctx, pred.where)
		if err != nil {
			return nil, h.execError(err)
		}
		return names, nil
	})
}

func (h *lokiHandler) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !isValidLabelName(name) {
		h.writeError(w, r, &lokiError{status: http.StatusBadRequest, msg: "invalid label name"})
		return
	}
	if err := parseForm(w, r); err != nil {
		h.writeError(w, r, &lokiError{status: http.StatusBadRequest, msg: "invalid request body"})
		return
	}
	q, apiErr := parseLokiSelector(r.Form.Get("query"))
	if apiErr != nil {
		h.writeError(w, r, apiErr)
		return
	}
	startNs, endNs, apiErr := lokiMetadataRange(r)
	if apiErr != nil {
		h.writeError(w, r, apiErr)
		return
	}
	h.withSandbox(w, r, startNs, endNs, true, func(ctx context.Context, rel *lokiRelation) (any, *lokiError) {
		if name == lokiJobLabel {
			return []string{lokiJobValue}, nil
		}
		pred := rel.predicates(q, startNs, endNs)
		if pred.matchNothing || !rel.has(name) {
			return []string{}, nil
		}
		values, err := rel.labelValues(ctx, name, pred.where, h.cfg.MaxEntries, q)
		if err != nil {
			return nil, h.execError(err)
		}
		return values, nil
	})
}

// withSandbox validates the tenant, opens the hardened per-request DuckDB
// sandbox over the tenant's log files, and invokes fn with the resulting
// relation. It centralizes the tenant/isolation flow shared by every endpoint.
// startNs/endNs prune the Parquet open set; omitMessage drops line bodies for
// label APIs.
func (h *lokiHandler) withSandbox(w http.ResponseWriter, r *http.Request, startNs, endNs int64, omitMessage bool, fn func(ctx context.Context, rel *lokiRelation) (any, *lokiError)) {
	ns := r.PathValue("ns")
	if !storeingest.ValidateTenant(ns) {
		h.log().Info("loki unknown tenant", "ns", ns, "status", http.StatusNotFound)
		http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
		return
	}
	absRoot, err := resolveSandboxTenantRoot(h.cfg.DataDir, filepath.Join(h.cfg.DataDir, ns))
	if err != nil {
		if errors.Is(err, errUnknownTenant) {
			h.log().Info("loki unknown tenant", "ns", ns, "status", http.StatusNotFound)
		} else {
			h.log().Error("loki tenant root", "ns", ns, "err", err)
		}
		http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.Timeout)
	defer cancel()

	if err := ctx.Err(); err != nil {
		h.writeError(w, r, h.execError(err))
		return
	}

	conn, cleanup, err := openLokiSandbox(ctx, absRoot, sandboxLimits{
		MemoryLimit: h.cfg.MemoryLimit,
		Threads:     h.cfg.Threads,
		ColdDir:     h.cfg.ColdDir,
	}, startNs, endNs, omitMessage, h.cfg.RecentLookback)
	if err != nil {
		if apiErr := h.execErrorIfCtx(ctx, err); apiErr != nil {
			h.writeError(w, r, apiErr)
			return
		}
		h.writeError(w, r, &lokiError{status: http.StatusInternalServerError, msg: "query failed", cause: err})
		return
	}
	defer cleanup()

	rel, err := newLokiRelation(ctx, conn, h.cfg.DataDir, ns)
	if err != nil {
		if apiErr := h.execErrorIfCtx(ctx, err); apiErr != nil {
			h.writeError(w, r, apiErr)
			return
		}
		h.writeError(w, r, &lokiError{status: http.StatusInternalServerError, msg: "query failed", cause: err})
		return
	}
	data, apiErr := fn(ctx, rel)
	if apiErr != nil {
		if err := ctx.Err(); err != nil {
			h.writeError(w, r, h.execError(err))
			return
		}
		h.writeError(w, r, apiErr)
		return
	}
	h.writeSuccess(w, data)
}

// lastLokiSandbox is test-only observation of the most recent Loki view build.
var lastLokiSandbox struct {
	mu      sync.Mutex
	viewSQL string
	openSet int
	omitMsg bool
}

func recordLokiSandbox(viewSQL string, openSet int, omitMsg bool) {
	lastLokiSandbox.mu.Lock()
	lastLokiSandbox.viewSQL = viewSQL
	lastLokiSandbox.openSet = openSet
	lastLokiSandbox.omitMsg = omitMsg
	lastLokiSandbox.mu.Unlock()
}

// openLokiSandbox opens a locked-down :memory: DuckDB connection whose only
// visible relation is the tenant's log windows, timestamped by landing time.
func openLokiSandbox(ctx context.Context, tenantRoot string, limits sandboxLimits, startNs, endNs int64, omitMessage bool, recentLookback time.Duration) (*sql.Conn, func(), error) {
	_, files, err := sandboxLokiLogs(tenantRoot, startNs, endNs, omitMessage, recentLookback, limits.ColdDir)
	if err != nil {
		return nil, nil, wrapSandboxErr(err)
	}
	effective := EffectiveSandboxThreads(limits.Threads, len(files))
	recordAppliedSandboxThreads(effective)
	limits.Threads = effective

	conn, cleanup, err := openSandboxConn(ctx, tenantRoot, limits, metrics.RoleLoki)
	if err != nil {
		return nil, nil, err
	}
	if err := attachLogsDuckDB(ctx, conn, files); err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(err)
	}
	if err := annotateDuckLogIngestTS(ctx, conn, files); err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(err)
	}
	opts := logsCatalogOpts{
		StartNs: startNs, EndNs: endNs, WithIngestTS: true, OmitMessage: omitMessage,
	}
	viewSQL, err := buildLogsRelationSQLMixed(files, opts)
	if err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE VIEW "+sandboxLogsView+" AS "+viewSQL); err != nil {
		if omitMessage {
			opts.OmitMessage = false
			viewSQL, err = buildLogsRelationSQLMixed(files, opts)
			if err != nil {
				cleanup()
				return nil, nil, wrapSandboxErr(err)
			}
			if _, err = conn.ExecContext(ctx, "CREATE VIEW "+sandboxLogsView+" AS "+viewSQL); err != nil {
				cleanup()
				return nil, nil, wrapSandboxErr(fmt.Errorf("create logs view: %w", err))
			}
			omitMessage = false
		} else {
			cleanup()
			return nil, nil, wrapSandboxErr(fmt.Errorf("create logs view: %w", err))
		}
	}
	recordLokiSandbox(viewSQL, len(files), omitMessage)
	if err := lockSandbox(ctx, conn); err != nil {
		cleanup()
		return nil, nil, err
	}
	return conn, cleanup, nil
}

// lokiRelation is the tenant's log relation plus its discovered column set. Log
// schemas vary per artifact and format, so which labels exist is a property of
// the data, resolved once per request.
type lokiRelation struct {
	conn    *sql.Conn
	columns []string
	dataDir string
	tenant  string
}

func newLokiRelation(ctx context.Context, conn *sql.Conn, dataDir, tenant string) (*lokiRelation, error) {
	rows, err := conn.QueryContext(ctx, "SELECT * FROM "+sandboxLogsView+" LIMIT 0")
	if err != nil {
		return nil, wrapSandboxErr(err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, wrapSandboxErr(err)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapSandboxErr(err)
	}
	return &lokiRelation{conn: conn, columns: cols, dataDir: dataDir, tenant: tenant}, nil
}

func (rel *lokiRelation) has(column string) bool {
	for _, c := range rel.columns {
		if c == column {
			return true
		}
	}
	return false
}

// lokiPredicate is a query's WHERE conjunction. matchNothing short-circuits a
// selector that cannot match any stream (for example an equality matcher on a
// label this tenant's logs do not carry), so no scan runs at all.
type lokiPredicate struct {
	where        []string
	matchNothing bool
}

// predicates pushes the time bounds, label matchers, and line filters into SQL.
// Label matchers on columns that do not exist — and on the synthetic job label —
// are decided in Go against the value they would have, which is how LogQL treats
// an absent label (the empty string).
func (rel *lokiRelation) predicates(q *logQLQuery, startNs, endNs int64) lokiPredicate {
	pred := lokiPredicate{where: []string{
		fmt.Sprintf("%s >= %d AND %s < %d", lokiTSColumn, startNs, lokiTSColumn, endNs),
	}}
	for _, m := range q.matchers {
		switch {
		case m.label == lokiJobLabel:
			if !m.matches(lokiJobValue) {
				pred.matchNothing = true
				return pred
			}
		case rel.has(m.label):
			pred.where = append(pred.where, lokiLabelPredicate(m.label, m))
		case !m.matches(""):
			pred.matchNothing = true
			return pred
		}
	}
	line := rel.lineExpr()
	for _, f := range q.filters {
		pred.where = append(pred.where, lokiLinePredicate(line, f))
	}
	return pred
}

// lineExpr is the SQL expression for a row's log line: the message when it has
// text, else the mined template, else empty. It must agree with the line the
// response reports, so filters never select a row whose rendered line differs.
func (rel *lokiRelation) lineExpr() string {
	parts := make([]string, 0, len(lokiLineColumns))
	for _, c := range lokiLineColumns {
		if rel.has(c) {
			parts = append(parts, fmt.Sprintf("NULLIF(CAST(%s AS VARCHAR), '')", quoteSQLIdent(c)))
		}
	}
	if len(parts) == 0 {
		return "''"
	}
	return "COALESCE(" + strings.Join(parts, ", ") + ", '')"
}

func lokiLabelPredicate(column string, m logQLMatcher) string {
	expr := fmt.Sprintf("COALESCE(CAST(%s AS VARCHAR), '')", quoteSQLIdent(column))
	lit := "'" + escapeSQLLiteral(m.value) + "'"
	switch m.op {
	case logQLNotEqual:
		return expr + " <> " + lit
	case logQLMatchRegex:
		return "regexp_full_match(" + expr + ", " + lit + ")"
	case logQLNotMatchRegex:
		return "NOT regexp_full_match(" + expr + ", " + lit + ")"
	default:
		return expr + " = " + lit
	}
}

func lokiLinePredicate(lineExpr string, f logQLLineFilter) string {
	lit := "'" + escapeSQLLiteral(f.value) + "'"
	switch f.op {
	case logQLNotEqual:
		return "NOT contains(" + lineExpr + ", " + lit + ")"
	case logQLMatchRegex:
		return "regexp_matches(" + lineExpr + ", " + lit + ")"
	case logQLNotMatchRegex:
		return "NOT regexp_matches(" + lineExpr + ", " + lit + ")"
	default:
		return "contains(" + lineExpr + ", " + lit + ")"
	}
}

// queryStreams reads at most limit entries ordered by landing time and groups
// them into streams by label set. The row cap is applied by SQL, so a query never
// materializes more entries than the caller asked for.
func (rel *lokiRelation) queryStreams(ctx context.Context, where []string, newestFirst bool, limit int) ([]lokiStreamJSON, error) {
	order := "ASC"
	if newestFirst {
		order = "DESC"
	}
	//nolint:gosec // G202: every fragment is built from validated identifiers and escaped literals.
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s ORDER BY %s %s LIMIT %d",
		sandboxLogsView, strings.Join(where, " AND "), lokiTSColumn, order, limit)
	rows, err := rel.conn.QueryContext(ctx, q)
	if err != nil {
		return nil, wrapSandboxErr(err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, wrapSandboxErr(err)
	}

	byLabels := make(map[string]*lokiStreamJSON)
	keys := make([]string, 0, 8)
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, wrapSandboxErr(err)
		}
		labels := lokiRowLabels(cols, vals)
		key := lokiLabelKey(labels)
		stream, ok := byLabels[key]
		if !ok {
			stream = &lokiStreamJSON{Stream: labels, Values: [][2]string{}}
			byLabels[key] = stream
			keys = append(keys, key)
		}
		stream.Values = append(stream.Values, [2]string{
			strconv.FormatInt(lokiRowTimestamp(cols, vals), 10),
			lokiRowLine(cols, vals),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, wrapSandboxErr(err)
	}
	sort.Strings(keys)
	out := make([]lokiStreamJSON, 0, len(keys))
	for _, k := range keys {
		out = append(out, *byLabels[k])
	}
	return out, nil
}

// labelNames reports the stream labels that actually carry a value in the
// selected window, plus the synthetic job label. One aggregate scan answers every
// candidate column, so the cost does not grow with the label count.
func (rel *lokiRelation) labelNames(ctx context.Context, where []string) ([]string, error) {
	candidates := rel.labelCandidates()
	names := []string{lokiJobLabel}
	if len(candidates) == 0 {
		return names, nil
	}
	projections := make([]string, len(candidates))
	for i, c := range candidates {
		projections[i] = fmt.Sprintf("count(NULLIF(CAST(%s AS VARCHAR), ''))", quoteSQLIdent(c))
	}
	//nolint:gosec // G202: projections are built from DuckDB-reported column identifiers.
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(projections, ", "), sandboxLogsView, strings.Join(where, " AND "))
	counts := make([]int64, len(candidates))
	ptrs := make([]any, len(candidates))
	for i := range counts {
		ptrs[i] = &counts[i]
	}
	if err := rel.conn.QueryRowContext(ctx, q).Scan(ptrs...); err != nil {
		return nil, wrapSandboxErr(err)
	}
	for i, c := range candidates {
		if counts[i] > 0 {
			names = append(names, c)
		}
	}
	sort.Strings(names)
	return names, nil
}

// labelValues reports the distinct values of one label in the selected window.
func (rel *lokiRelation) labelValues(ctx context.Context, name string, where []string, limit int, sel *logQLQuery) ([]string, error) {
	if logmeta.IsIndexedLabel(name) && selectorAllowsLabelIndex(sel) {
		// Index reads are file/DuckDB helpers without a request-scoped DB handle.
		vals, err := labelValuesFromIndex(rel.dataDir, rel.tenant, name, limit) //nolint:contextcheck // logmeta index path has no conn ctx
		if err != nil {
			return nil, wrapSandboxErr(err)
		}
		return vals, nil
	}
	expr := fmt.Sprintf("NULLIF(CAST(%s AS VARCHAR), '')", quoteSQLIdent(name))
	//nolint:gosec // G202: name is a DuckDB-reported column identifier and the literals are escaped.
	sqlQ := fmt.Sprintf("SELECT DISTINCT %s AS v FROM %s WHERE %s AND %s IS NOT NULL ORDER BY v LIMIT %d",
		expr, sandboxLogsView, strings.Join(where, " AND "), expr, limit)
	recordLabelValuesObservation("scan", sqlQ)
	rows, err := rel.conn.QueryContext(ctx, sqlQ)
	if err != nil {
		return nil, wrapSandboxErr(err)
	}
	defer func() { _ = rows.Close() }()
	out := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, wrapSandboxErr(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapSandboxErr(err)
	}
	return out, nil
}

// labelCandidates are the columns eligible to become stream labels: everything
// except the ingest-time column, the log line itself, and names that are not
// legal label names.
func (rel *lokiRelation) labelCandidates() []string {
	out := make([]string, 0, len(rel.columns))
	for _, c := range rel.columns {
		if c == lokiTSColumn || c == lokiMessageColumn || !isValidLabelName(c) {
			continue
		}
		out = append(out, c)
	}
	return out
}

// lokiRowLabels builds a row's stream labels: text columns become labels, a
// summarized count becomes a label string, and every stream carries the synthetic
// job label. Empty and NULL values are omitted — an absent label is how Loki
// spells "no value".
func lokiRowLabels(cols []string, vals []any) map[string]string {
	labels := map[string]string{lokiJobLabel: lokiJobValue}
	for i, c := range cols {
		if c == lokiTSColumn || c == lokiMessageColumn || !isValidLabelName(c) {
			continue
		}
		v, ok := lokiLabelValue(c, vals[i])
		if !ok || v == "" {
			continue
		}
		labels[c] = v
	}
	return labels
}

// lokiLabelValue renders a cell as a label value. Text columns are labels;
// numbers are not, except the summarized count, which is the one number that
// identifies a stream rather than measuring it.
func lokiLabelValue(column string, v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", false
	case string:
		return x, true
	case []byte:
		return string(x), true
	case int64:
		if column == lokiCountColumn {
			return strconv.FormatInt(x, 10), true
		}
	case int32:
		if column == lokiCountColumn {
			return strconv.FormatInt(int64(x), 10), true
		}
	case float64:
		if column == lokiCountColumn {
			return strconv.FormatFloat(x, 'f', -1, 64), true
		}
	}
	return "", false
}

// lokiRowLine renders the log line: the message when it has text, else the mined
// template, else empty.
func lokiRowLine(cols []string, vals []any) string {
	for _, name := range lokiLineColumns {
		for i, c := range cols {
			if c != name {
				continue
			}
			if s, ok := lokiLabelValue(c, vals[i]); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

func lokiRowTimestamp(cols []string, vals []any) int64 {
	for i, c := range cols {
		if c != lokiTSColumn {
			continue
		}
		if n, ok := vals[i].(int64); ok {
			return n
		}
	}
	return 0
}

// lokiLabelKey renders a label set as a stable, sortable identity so equal label
// sets group into one stream and streams are returned in a deterministic order.
func lokiLabelKey(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for n := range labels {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for i, n := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(n)
		b.WriteString("=")
		b.WriteString(strconv.Quote(labels[n]))
	}
	return b.String()
}

// parseLokiSelector parses the LogQL subset, mapping both unsupported syntax and
// parse errors to a 400 carrying the reason.
func parseLokiSelector(expr string) (*logQLQuery, *lokiError) {
	q, err := parseLogQL(expr)
	if err != nil {
		return nil, &lokiError{status: http.StatusBadRequest, msg: err.Error()}
	}
	return q, nil
}

// lokiQueryRange resolves a query_range window: end defaults to now and start to
// one hour earlier, matching the Loki API defaults.
func lokiQueryRange(r *http.Request) (int64, int64, *lokiError) {
	now := time.Now().UnixNano()
	endNs, err := parseLokiTimeNanos(r.Form.Get("end"), now)
	if err != nil {
		return 0, 0, &lokiError{status: http.StatusBadRequest, msg: "invalid end"}
	}
	startNs, err := parseLokiTimeNanos(r.Form.Get("start"), endNs-int64(defaultLokiRange))
	if err != nil {
		return 0, 0, &lokiError{status: http.StatusBadRequest, msg: "invalid start"}
	}
	if endNs < startNs {
		return 0, 0, &lokiError{status: http.StatusBadRequest, msg: "end before start"}
	}
	return startNs, endNs, nil
}

// lokiMetadataRange resolves the window for the label endpoints. Omitting the
// bounds means "everything stored", so a label browser is never empty just
// because the logs are older than a default window.
func lokiMetadataRange(r *http.Request) (int64, int64, *lokiError) {
	startNs, err := parseLokiTimeNanos(r.Form.Get("start"), 0)
	if err != nil {
		return 0, 0, &lokiError{status: http.StatusBadRequest, msg: "invalid start"}
	}
	endNs, err := parseLokiTimeNanos(r.Form.Get("end"), time.Now().UnixNano())
	if err != nil {
		return 0, 0, &lokiError{status: http.StatusBadRequest, msg: "invalid end"}
	}
	if endNs < startNs {
		return 0, 0, &lokiError{status: http.StatusBadRequest, msg: "end before start"}
	}
	return startNs, endNs, nil
}

// parseLokiLimit reads the optional entry limit. Empty means the Loki default
// page size; the server cap always wins so one request stays bounded.
func parseLokiLimit(raw string, maxEntries int) (int, *lokiError) {
	limit := defaultLokiLimit
	if s := strings.TrimSpace(raw); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return 0, &lokiError{status: http.StatusBadRequest, msg: "invalid limit"}
		}
		limit = n
	}
	if limit > maxEntries {
		limit = maxEntries
	}
	return limit, nil
}

// execError maps sandbox/context failures onto the Loki envelope. The HTTP
// body stays generic so clients never see DuckDB paths; the cause is logged
// with truncated LogQL.
func (*lokiHandler) execError(err error) *lokiError {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &lokiError{status: http.StatusServiceUnavailable, msg: "query timed out", cause: err}
	case httperr.IsCanceled(err):
		return &lokiError{status: httperr.StatusClientClosed, msg: "query was canceled", cause: err}
	default:
		return &lokiError{status: http.StatusInternalServerError, msg: "query failed", cause: err}
	}
}

func (h *lokiHandler) execErrorIfCtx(ctx context.Context, err error) *lokiError {
	if httperr.IsCanceled(err) || httperr.IsCanceled(ctx.Err()) {
		return h.execError(context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return h.execError(context.DeadlineExceeded)
	}
	return nil
}

func (h *lokiHandler) writeSuccess(w http.ResponseWriter, data any) {
	h.writeJSON(w, http.StatusOK, lokiResponse{Status: "success", Data: data})
}

func (h *lokiHandler) writeError(w http.ResponseWriter, r *http.Request, e *lokiError) {
	if e == nil {
		return
	}
	ns := ""
	q := ""
	if r != nil {
		ns = r.PathValue("ns")
		q = lokiQueryText(r)
	}
	logErr := e.cause
	if logErr == nil && e.msg != "" {
		logErr = errors.New(e.msg)
	}
	switch {
	case e.status >= 500:
		h.log().Error("loki query", "ns", ns, "status", e.status, "query", truncateQuery(q, queryLogCap), "err", logErr)
	case e.status == http.StatusBadRequest:
		h.log().Warn("loki query", "ns", ns, "status", e.status, "query", truncateQuery(q, queryLogCap), "err", logErr)
	}
	h.writeJSON(w, e.status, lokiResponse{Status: "error", Error: e.msg})
}

func lokiQueryText(r *http.Request) string {
	if r.PostForm != nil {
		if v := r.PostForm.Get("query"); v != "" {
			return v
		}
	}
	if r.Form != nil {
		if v := r.Form.Get("query"); v != "" {
			return v
		}
	}
	return r.URL.Query().Get("query")
}

func (h *lokiHandler) writeJSON(w http.ResponseWriter, status int, resp lokiResponse) {
	payload, err := json.Marshal(resp)
	if err != nil {
		h.log().Error("loki marshal", "err", err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(payload); err != nil {
		h.log().Error("loki write", "err", err)
	}
}

// log returns a usable logger even when the caller supplied none, so no code path
// has to nil-check before recording a failure.
func (h *lokiHandler) log() *slog.Logger {
	if h.logger != nil {
		return h.logger
	}
	return slog.New(slog.NewTextHandler(nopWriter{}, nil))
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// quoteSQLIdent quotes a column identifier so a name that collides with SQL
// syntax (or contains a quote) cannot change the shape of the query.
func quoteSQLIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
