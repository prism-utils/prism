package query

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/httperr"
	storeingest "github.com/prism-utils/prism/internal/store/ingest"
	storetenant "github.com/prism-utils/prism/internal/store/tenant"
)

const promQLMaxBodyBytes = 1 << 20 // 1 MiB form body cap for POST queries.

// Prometheus API errorType values (see prometheus.io querying/api).
const (
	errTypeBadData   = "bad_data"
	errTypeExecution = "execution"
	errTypeTimeout   = "timeout"
	errTypeCanceled  = "canceled"
	errTypeInternal  = "internal"
)

// defaultMinTime / defaultMaxTime bound label and series endpoints when the
// caller omits start/end. prism stamps ingest time onto `ts`, so [1970, ~2970]
// covers every stored sample without overflowing DuckDB's BIGINT epoch math.
var (
	defaultMinTime = time.Unix(0, 0).UTC()
	defaultMaxTime = time.Unix(0, 0).UTC().AddDate(1000, 0, 0)
)

// apiResponse is the exact Prometheus HTTP API JSON envelope.
type apiResponse struct {
	Status    string `json:"status"`
	Data      any    `json:"data,omitempty"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

type queryData struct {
	ResultType string `json:"resultType"`
	Result     any    `json:"result"`
}

type vectorSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

type matrixSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values"`
}

// promQLHandler serves the Prometheus-compatible read API for one store.
type promQLHandler struct {
	cfg    *PromQLConfig
	eng    *engine.Engine
	qeng   *promql.Engine
	logger *slog.Logger
}

// PromQLHandler returns an http.Handler that serves the Prometheus API surface
// (query, query_range, series, labels, label/<name>/values). One handler serves
// every route via a single dispatcher so RBAC, cluster routing, and the /sql
// queue can wrap it uniformly.
func PromQLHandler(cfg *PromQLConfig, eng *engine.Engine, logger *slog.Logger) http.Handler {
	if cfg == nil {
		cfg = &PromQLConfig{}
	}
	c := *cfg
	c.withDefaults()
	// withDefaults fills every bound, so Validate is defensive; log rather than
	// fail so a store never silently runs an out-of-range limit.
	if err := c.Validate(); err != nil && logger != nil {
		logger.Warn("promql config invalid after defaults", "err", err)
	}
	h := &promQLHandler{cfg: &c, eng: eng, qeng: newPromQLEngine(&c, logger), logger: logger}
	return http.HandlerFunc(h.serve)
}

func (h *promQLHandler) serve(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case strings.HasSuffix(path, "/api/v1/query"):
		h.handleInstant(w, r)
	case strings.HasSuffix(path, "/api/v1/query_range"):
		h.handleRange(w, r)
	case strings.HasSuffix(path, "/api/v1/series"):
		h.handleSeries(w, r)
	case strings.HasSuffix(path, "/api/v1/labels"):
		h.handleLabelNames(w, r)
	case strings.Contains(path, "/api/v1/label/") && strings.HasSuffix(path, "/values"):
		h.handleLabelValues(w, r)
	default:
		http.NotFound(w, r)
	}
}

// withSandbox validates the tenant, opens the hardened per-request DuckDB
// sandbox, and invokes fn with a storage.Queryable over the tenant metrics view.
// It centralizes the tenant/isolation/hot-only flow shared by every endpoint.
func (h *promQLHandler) withSandbox(w http.ResponseWriter, r *http.Request, fn func(ctx context.Context, q storage.Queryable) (any, *apiError)) {
	ns := r.PathValue("ns")
	if !storeingest.ValidateTenant(ns) {
		http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
		return
	}
	tenantRoot := filepath.Join(h.cfg.DataDir, ns)
	absRoot, err := resolveSandboxTenantRoot(h.cfg.DataDir, tenantRoot)
	if err != nil {
		if errors.Is(err, errUnknownTenant) {
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}
		h.logger.Error("promql tenant root", "ns", ns, "err", err)
		http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.Timeout)
	defer cancel()

	if err := ctx.Err(); err != nil {
		h.writeError(w, execError(err))
		return
	}

	// A writer flushes fresh hot rows so its own reads are current; a read-only
	// replica serves the writer's snapshot as-is and never writes here.
	if h.cfg.RunJobs && h.eng != nil {
		//nolint:contextcheck // snapshot export uses engine-internal context; request ctx applies to the sandbox query below.
		if err := h.eng.ExportHotSnapshot(ns); err != nil {
			h.logger.Error("promql hot snapshot", "ns", ns, "err", err)
			h.writeError(w, &apiError{status: http.StatusInternalServerError, typ: errTypeInternal, msg: "query failed"})
			return
		}
		if err := ctx.Err(); err != nil {
			h.writeError(w, execError(err))
			return
		}
	}

	// hot_only can only tighten scope: a request may force the hot snapshot, but
	// cannot widen a store that is already globally hot-only.
	hotOnly := h.cfg.HotOnly || wantsHotOnly(r)
	conn, cleanup, err := prepareMetricsSandboxConn(ctx, absRoot, hotOnly, sandboxLimits{
		MemoryLimit: h.cfg.MemoryLimit,
		Threads:     h.cfg.Threads,
	})
	if err != nil {
		if apiErr := execErrorIfCtx(ctx, err); apiErr != nil {
			h.writeError(w, apiErr)
			return
		}
		h.logger.Error("promql sandbox", "ns", ns, "err", err)
		h.writeError(w, &apiError{status: http.StatusInternalServerError, typ: errTypeInternal, msg: "query failed"})
		return
	}
	defer cleanup()

	q := &sandboxQueryable{conn: conn, view: sandboxMetricsView, maxSamples: h.cfg.MaxSamples}
	data, apiErr := fn(ctx, q)
	if apiErr != nil {
		if err := ctx.Err(); err != nil {
			h.writeError(w, execError(err))
			return
		}
		h.writeError(w, apiErr)
		return
	}
	h.writeSuccess(w, data)
}

func (h *promQLHandler) handleInstant(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(w, r); err != nil {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid request body"})
		return
	}
	expr := strings.TrimSpace(r.Form.Get("query"))
	if expr == "" {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "missing query"})
		return
	}
	ts, err := parseTimeParam(r.Form.Get("time"), time.Now())
	if err != nil {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid time"})
		return
	}
	h.withSandbox(w, r, func(ctx context.Context, q storage.Queryable) (any, *apiError) {
		res, err := execInstant(ctx, h.qeng, q, expr, ts)
		if err != nil {
			return nil, parseExprError(err)
		}
		if res.Err != nil {
			return nil, execError(res.Err)
		}
		return resultToData(res)
	})
}

func (h *promQLHandler) handleRange(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(w, r); err != nil {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid request body"})
		return
	}
	expr := strings.TrimSpace(r.Form.Get("query"))
	if expr == "" {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "missing query"})
		return
	}
	start, err := parseTimeParam(r.Form.Get("start"), time.Time{})
	if err != nil || start.IsZero() {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid start"})
		return
	}
	end, err := parseTimeParam(r.Form.Get("end"), time.Time{})
	if err != nil || end.IsZero() {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid end"})
		return
	}
	step, err := parseDurationParam(r.Form.Get("step"))
	if err != nil || step <= 0 {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid step"})
		return
	}
	if end.Before(start) {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "end before start"})
		return
	}
	if points := end.Sub(start) / step; points > time.Duration(h.cfg.MaxPoints) {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "exceeded maximum resolution: reduce step or time range"})
		return
	}
	h.withSandbox(w, r, func(ctx context.Context, q storage.Queryable) (any, *apiError) {
		res, err := execRange(ctx, h.qeng, q, expr, start, end, step)
		if err != nil {
			return nil, parseExprError(err)
		}
		if res.Err != nil {
			return nil, execError(res.Err)
		}
		return resultToData(res)
	})
}

func (h *promQLHandler) handleSeries(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(w, r); err != nil {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid request body"})
		return
	}
	matcherSets, apiErr := parseMatchParams(r.Form["match[]"], true)
	if apiErr != nil {
		h.writeError(w, apiErr)
		return
	}
	limit, apiErr := parseLimitParam(r)
	if apiErr != nil {
		h.writeError(w, apiErr)
		return
	}
	mint, maxt, apiErr := timeRange(r)
	if apiErr != nil {
		h.writeError(w, apiErr)
		return
	}
	h.withSandbox(w, r, func(ctx context.Context, q storage.Queryable) (any, *apiError) {
		querier, err := q.Querier(mint, maxt)
		if err != nil {
			return nil, execError(err)
		}
		defer func() { _ = querier.Close() }()
		sq, ok := querier.(*sandboxQuerier)
		if !ok {
			return nil, &apiError{status: http.StatusInternalServerError, typ: errTypeInternal, msg: "query failed"}
		}
		seen := map[string]struct{}{}
		out := []map[string]string{}
		// Each match[] is a separate selector whose results union (OR), so /series
		// reads distinct series per selector rather than every sample.
		for _, ms := range matcherSets {
			lblsList, err := sq.seriesLabels(ctx, ms, limit)
			if err != nil {
				return nil, execError(err)
			}
			for _, lbls := range lblsList {
				key := lbls.String()
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
				out = append(out, lbls.Map())
				if limit > 0 && len(out) >= limit {
					return out, nil
				}
			}
		}
		return out, nil
	})
}

func (h *promQLHandler) handleLabelNames(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(w, r); err != nil {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid request body"})
		return
	}
	matcherSets, apiErr := parseMatchParams(r.Form["match[]"], false)
	if apiErr != nil {
		h.writeError(w, apiErr)
		return
	}
	limit, apiErr := parseLimitParam(r)
	if apiErr != nil {
		h.writeError(w, apiErr)
		return
	}
	mint, maxt, apiErr := timeRange(r)
	if apiErr != nil {
		h.writeError(w, apiErr)
		return
	}
	h.withSandbox(w, r, func(ctx context.Context, q storage.Queryable) (any, *apiError) {
		querier, err := q.Querier(mint, maxt)
		if err != nil {
			return nil, execError(err)
		}
		defer func() { _ = querier.Close() }()
		seen := map[string]struct{}{}
		hints := &storage.LabelHints{Limit: limit}
		for _, ms := range matcherSetsOrNil(matcherSets) {
			names, _, err := querier.LabelNames(ctx, hints, ms...)
			if err != nil {
				return nil, execError(err)
			}
			for _, n := range names {
				seen[n] = struct{}{}
			}
		}
		return capSorted(seen, limit), nil
	})
}

func (h *promQLHandler) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !isValidLabelName(name) {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid label name"})
		return
	}
	if err := parseForm(w, r); err != nil {
		h.writeError(w, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid request body"})
		return
	}
	matcherSets, apiErr := parseMatchParams(r.Form["match[]"], false)
	if apiErr != nil {
		h.writeError(w, apiErr)
		return
	}
	limit, apiErr := parseLimitParam(r)
	if apiErr != nil {
		h.writeError(w, apiErr)
		return
	}
	mint, maxt, apiErr := timeRange(r)
	if apiErr != nil {
		h.writeError(w, apiErr)
		return
	}
	h.withSandbox(w, r, func(ctx context.Context, q storage.Queryable) (any, *apiError) {
		querier, err := q.Querier(mint, maxt)
		if err != nil {
			return nil, execError(err)
		}
		defer func() { _ = querier.Close() }()
		seen := map[string]struct{}{}
		hints := &storage.LabelHints{Limit: limit}
		for _, ms := range matcherSetsOrNil(matcherSets) {
			values, _, err := querier.LabelValues(ctx, name, hints, ms...)
			if err != nil {
				return nil, execError(err)
			}
			for _, v := range values {
				seen[v] = struct{}{}
			}
		}
		return capSorted(seen, limit), nil
	})
}

// resultToData converts a promql result into the Prometheus data envelope.
func resultToData(res *promql.Result) (any, *apiError) {
	switch v := res.Value.(type) {
	case promql.Vector:
		result := make([]vectorSample, 0, len(v))
		for _, s := range v {
			result = append(result, vectorSample{
				Metric: metricMap(s.Metric, s.DropName),
				Value:  [2]any{tsSeconds(s.T), formatValue(s.F)},
			})
		}
		return queryData{ResultType: "vector", Result: result}, nil
	case promql.Matrix:
		result := make([]matrixSeries, 0, len(v))
		for _, s := range v {
			vals := make([][2]any, 0, len(s.Floats))
			for _, p := range s.Floats {
				vals = append(vals, [2]any{tsSeconds(p.T), formatValue(p.F)})
			}
			result = append(result, matrixSeries{Metric: metricMap(s.Metric, s.DropName), Values: vals})
		}
		return queryData{ResultType: "matrix", Result: result}, nil
	case promql.Scalar:
		return queryData{ResultType: "scalar", Result: [2]any{tsSeconds(v.T), formatValue(v.V)}}, nil
	case promql.String:
		return queryData{ResultType: "string", Result: [2]any{tsSeconds(v.T), v.V}}, nil
	default:
		return nil, &apiError{status: http.StatusInternalServerError, typ: errTypeInternal, msg: "unsupported result type"}
	}
}

func metricMap(lbls labels.Labels, dropName bool) map[string]string {
	m := lbls.Map()
	if dropName {
		delete(m, metricNameLabel)
	}
	return m
}

func tsSeconds(ms int64) float64 { return float64(ms) / 1000 }

// formatValue renders a sample value the way Prometheus does: shortest decimal,
// with the sentinel strings for the non-finite values PromQL can produce.
func formatValue(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	default:
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
}

// apiError carries an HTTP status plus the Prometheus errorType/message.
type apiError struct {
	status int
	typ    string
	msg    string
}

func parseExprError(err error) *apiError {
	return &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: err.Error()}
}

func execError(err error) *apiError {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &apiError{status: http.StatusServiceUnavailable, typ: errTypeTimeout, msg: "query timed out"}
	case httperr.IsCanceled(err):
		return &apiError{status: httperr.StatusClientClosed, typ: errTypeCanceled, msg: "query was canceled"}
	default:
		return &apiError{status: http.StatusUnprocessableEntity, typ: errTypeExecution, msg: err.Error()}
	}
}

// execErrorIfCtx maps a sandbox failure onto timeout/cancel when the request
// context is already done, including driver errors that omit the cancel cause.
func execErrorIfCtx(ctx context.Context, err error) *apiError {
	if httperr.IsCanceled(err) || httperr.IsCanceled(ctx.Err()) {
		return execError(context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return execError(context.DeadlineExceeded)
	}
	return nil
}

func (h *promQLHandler) writeSuccess(w http.ResponseWriter, data any) {
	h.writeJSON(w, http.StatusOK, apiResponse{Status: "success", Data: data})
}

func (h *promQLHandler) writeError(w http.ResponseWriter, e *apiError) {
	h.writeJSON(w, e.status, apiResponse{Status: "error", ErrorType: e.typ, Error: e.msg})
}

func (h *promQLHandler) writeJSON(w http.ResponseWriter, status int, resp apiResponse) {
	payload, err := json.Marshal(resp)
	if err != nil {
		h.logger.Error("promql marshal", "err", err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(payload); err != nil {
		h.logger.Error("promql write", "err", err)
	}
}

func parseForm(w http.ResponseWriter, r *http.Request) error {
	r.Body = http.MaxBytesReader(w, r.Body, promQLMaxBodyBytes)
	return r.ParseForm()
}

// parseTimeParam accepts a Unix timestamp (seconds, optionally fractional) or an
// RFC3339 string, matching the Prometheus API. Empty returns def.
func parseTimeParam(s string, def time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return def, nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), nil
	}
	return time.Parse(time.RFC3339, s)
}

// parseDurationParam accepts a Prometheus duration ("15s", "1m") or a plain
// number of seconds, matching the step parameter's accepted forms.
func parseDurationParam(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty duration")
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return time.Duration(f * float64(time.Second)), nil
}

// timeRange resolves optional start/end params to a ms range, defaulting to the
// full stored range when either bound is omitted.
func timeRange(r *http.Request) (int64, int64, *apiError) {
	start, err := parseTimeParam(r.Form.Get("start"), defaultMinTime)
	if err != nil {
		return 0, 0, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid start"}
	}
	end, err := parseTimeParam(r.Form.Get("end"), defaultMaxTime)
	if err != nil {
		return 0, 0, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid end"}
	}
	return start.UnixMilli(), end.UnixMilli(), nil
}

// parseMatchParams parses match[] selectors. When required is true, at least one
// selector must be present (the /series contract).
func parseMatchParams(raw []string, required bool) ([][]*labels.Matcher, *apiError) {
	if len(raw) == 0 {
		if required {
			return nil, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "no match[] parameter provided"}
		}
		return nil, nil
	}
	p := parser.NewParser(parser.Options{})
	var sets [][]*labels.Matcher
	for _, sel := range raw {
		ms, err := p.ParseMetricSelector(sel)
		if err != nil {
			return nil, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: err.Error()}
		}
		sets = append(sets, ms)
	}
	return sets, nil
}

// matcherSetsOrNil yields the parsed selectors, or a single nil selector when
// none were supplied, so callers can union results with one loop. Repeated
// match[] selectors union (OR), matching the Prometheus metadata API.
func matcherSetsOrNil(sets [][]*labels.Matcher) [][]*labels.Matcher {
	if len(sets) == 0 {
		return [][]*labels.Matcher{nil}
	}
	return sets
}

// parseLimitParam reads the optional Prometheus `limit` result cap. Empty or 0
// means no limit; a negative or non-numeric value is rejected.
func parseLimitParam(r *http.Request) (int, *apiError) {
	s := strings.TrimSpace(r.Form.Get("limit"))
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0, &apiError{status: http.StatusBadRequest, typ: errTypeBadData, msg: "invalid limit"}
	}
	return n, nil
}

// wantsHotOnly reports whether the request opts into hot-only evaluation via the
// prism `hot_only` extension param. Rulers (prism-alert) set it so recurring
// evaluations never scan cold Parquet tiers. It reuses the package parseBool so
// accepted spellings never drift; an empty or malformed value is simply false
// (the param is an optional tightening, not a required flag). Callers rely on
// r.Form, which every handler populates via parseForm before entering withSandbox.
func wantsHotOnly(r *http.Request) bool {
	v, err := parseBool(r.Form.Get("hot_only"))
	return err == nil && v
}

func isValidLabelName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '_':
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case i > 0 && c >= '0' && c <= '9':
		default:
			return false
		}
	}
	return true
}
