package admin

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/config"
	"github.com/prism-utils/prism/internal/store/merge"
	storetenant "github.com/prism-utils/prism/internal/store/tenant"
)

// CompactRoutePattern returns the ServeMux pattern for tenant compact.
func CompactRoutePattern() string {
	return "POST /admin/tenants/{ns}/compact"
}

// Compactor plans and queues a compact action for one tenant.
type Compactor interface {
	// PlanCompact returns the next bounded pack for tenant, if any.
	PlanCompact(tenant string, spec merge.CompactSpec) (merge.MergeAction, bool, error)
	// EnqueueCompact stores action for the next merge tick of tenant.
	EnqueueCompact(tenant string, action merge.MergeAction)
	// CompactPolicy looks up a named policy loaded at process start.
	CompactPolicy(name string) (merge.CompactSpec, bool)
}

type compactRequest struct {
	Policy     string `json:"policy"`
	DryRun     bool   `json:"dryRun"`
	Tier       *int   `json:"tier"`
	OlderThan  string `json:"olderThan"`
	NewerThan  string `json:"newerThan"`
	From       string `json:"from"`
	To         string `json:"to"`
	Bucket     string `json:"bucket"`
	MaxSources int    `json:"maxSources"`
	MaxBytes   string `json:"maxBytes"`
}

type compactResponse struct {
	Sources  []string `json:"sources"`
	Bytes    int64    `json:"bytes"`
	DestTier int      `json:"destTier"`
	DryRun   bool     `json:"dryRun,omitempty"`
	Queued   bool     `json:"queued,omitempty"`
}

// CompactHandler serves POST /admin/tenants/{ns}/compact.
func CompactHandler(cfg *Config, c Compactor, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")
		if !storetenant.TenantAllowed(ns) {
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}
		if !cfg.RunJobs {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if c == nil {
			http.Error(w, "compact unavailable", http.StatusServiceUnavailable)
			return
		}
		var req compactRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "missing compact window", http.StatusBadRequest)
			return
		}
		spec, err := compactSpecFromRequest(c, &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		action, ok, err := c.PlanCompact(ns, spec)
		if err != nil {
			logger.Error("compact plan", "ns", ns, "err", err)
			http.Error(w, "compact failed", http.StatusInternalServerError)
			return
		}
		resp := compactPlanResponse(action, ok)
		if req.DryRun {
			resp.DryRun = true
			writeCompactJSON(w, http.StatusOK, resp)
			return
		}
		if ok {
			c.EnqueueCompact(ns, action)
			resp.Queued = true
		}
		writeCompactJSON(w, http.StatusAccepted, resp)
	})
}

func compactSpecFromRequest(c Compactor, req *compactRequest) (merge.CompactSpec, error) {
	hasWindow := strings.TrimSpace(req.Policy) != "" ||
		strings.TrimSpace(req.OlderThan) != "" ||
		strings.TrimSpace(req.From) != "" ||
		strings.TrimSpace(req.To) != ""
	if !hasWindow {
		return merge.CompactSpec{}, errMissingWindow
	}
	if name := strings.TrimSpace(req.Policy); name != "" {
		spec, ok := c.CompactPolicy(name)
		if !ok {
			return merge.CompactSpec{}, errUnknownPolicy
		}
		return spec, nil
	}
	return inlineCompactSpec(req)
}

func inlineCompactSpec(req *compactRequest) (merge.CompactSpec, error) {
	spec := merge.CompactSpec{
		MaxSources: merge.DefaultCatchupMaxSources,
		MaxBytes:   merge.DefaultCatchupMaxBytes,
		Bucket:     merge.BucketNone,
	}
	if req.Tier != nil {
		spec.Tier = *req.Tier
	}
	if req.MaxSources > 0 {
		spec.MaxSources = req.MaxSources
	}
	if strings.TrimSpace(req.MaxBytes) != "" {
		n, err := config.ParseByteSize(req.MaxBytes)
		if err != nil {
			return merge.CompactSpec{}, err
		}
		spec.MaxBytes = n
	}
	older, err := parseAdminDuration(req.OlderThan)
	if err != nil {
		return merge.CompactSpec{}, err
	}
	spec.OlderThan = older
	newer, err := parseAdminDuration(req.NewerThan)
	if err != nil {
		return merge.CompactSpec{}, err
	}
	spec.NewerThan = newer
	from, err := parseAdminTime(req.From)
	if err != nil {
		return merge.CompactSpec{}, err
	}
	spec.From = from
	to, err := parseAdminTime(req.To)
	if err != nil {
		return merge.CompactSpec{}, err
	}
	spec.To = to
	if strings.TrimSpace(req.Bucket) != "" {
		switch strings.ToLower(strings.TrimSpace(req.Bucket)) {
		case string(merge.BucketNone), string(merge.BucketHour), string(merge.BucketDay):
			spec.Bucket = merge.Bucket(strings.ToLower(strings.TrimSpace(req.Bucket)))
		default:
			return merge.CompactSpec{}, errBadBucket
		}
	}
	return spec, nil
}

func compactPlanResponse(action merge.MergeAction, ok bool) compactResponse {
	if !ok {
		return compactResponse{}
	}
	sources := make([]string, len(action.Sources))
	var sum int64
	for i, s := range action.Sources {
		sources[i] = s.Path
		sum += s.Bytes
	}
	return compactResponse{Sources: sources, Bytes: sum, DestTier: action.DestTier}
}

func writeCompactJSON(w http.ResponseWriter, status int, resp compactResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func parseAdminDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

func parseAdminTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, s)
}

type compactError string

func (e compactError) Error() string { return string(e) }

const (
	errMissingWindow compactError = "missing compact window"
	errUnknownPolicy compactError = "unknown policy"
	errBadBucket     compactError = "bucket must be none, hour, or day"
)
