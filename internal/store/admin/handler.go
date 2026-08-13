package admin

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/prism-utils/prism/internal/store/authz"
	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/seed"
	storetenant "github.com/prism-utils/prism/internal/store/tenant"
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
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}
		// Read replicas own no tenant writes; writers create engine + seeds.
		if !cfg.RunJobs {
			w.WriteHeader(http.StatusNoContent)
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
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}
		var resp StatsResponse
		if cfg.RBACEnabled {
			scope, ok := authz.StatsScopeFromContext(r.Context())
			if !ok {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			if ns != "" {
				resp = BuildStatsResponse(cfg, eng, ns)
			} else {
				resp = BuildStatsResponseScoped(cfg, eng, scope)
			}
		} else {
			resp = BuildStatsResponse(cfg, eng, ns)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	})
}
