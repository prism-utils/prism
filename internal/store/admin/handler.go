package admin

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/seed"
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
)

// EnsureRoutePattern returns the ServeMux pattern for tenant ensure.
func EnsureRoutePattern() string {
	return "POST /admin/tenants/{ns}/ensure"
}

// StatsRoutePattern returns the ServeMux pattern for billing stats.
func StatsRoutePattern() string {
	return "GET /stats"
}

// EnsureHandler serves POST /admin/tenants/{ns}/ensure.
func EnsureHandler(cfg *Config, eng *engine.Engine, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")
		if !storetenant.TenantAllowed(ns) {
			http.Error(w, "unknown tenant", http.StatusNotFound)
			return
		}
		if _, err := eng.DB(ns); err != nil {
			logger.Error("ensure tenant engine", "ns", ns, "err", err)
			http.Error(w, "ensure failed", http.StatusInternalServerError)
			return
		}
		if err := seed.EnsureMetricsRawSeedForTenant(cfg.DataDir, ns); err != nil {
			logger.Error("ensure tenant seed", "ns", ns, "err", err)
			http.Error(w, "ensure failed", http.StatusInternalServerError)
			return
		}
		if err := seed.EnsureTieredLayoutForTenant(cfg.DataDir, ns); err != nil {
			logger.Error("ensure tiered layout", "ns", ns, "err", err)
			http.Error(w, "ensure failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

// StatsHandler serves GET /stats?ns=.
func StatsHandler(cfg *Config, eng *engine.Engine) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := r.URL.Query().Get("ns")
		if ns != "" && !storetenant.TenantAllowed(ns) {
			http.Error(w, "unknown tenant", http.StatusNotFound)
			return
		}
		resp := BuildStatsResponse(cfg, eng, ns)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	})
}
