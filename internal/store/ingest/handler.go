package ingest

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/elk-utilities/prism/internal/store/engine"
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
)

// IngestRoutePattern returns the ServeMux pattern for the ingest POST route.
func IngestRoutePattern(prefix string) string {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return "POST /{ns}/ingest/{artifact}"
	}
	return "POST " + prefix + "/{ns}/ingest/{artifact}"
}

// Handler serves POST ingest requests with the validation chain documented in
// docs/STORE.md: auth, tenant, artifact, body size, then engine landing.
func Handler(cfg *Config, eng *engine.Engine, logger *slog.Logger) http.Handler {
	auth := NewAuthenticator(cfg)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")
		artifact := r.PathValue("artifact")

		authOK, authTenant := auth.Authenticate(r)
		if !authOK {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if authTenant != "" && authTenant != ns {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		if !ValidateTenant(ns) {
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}
		if !ValidateArtifact(artifact, cfg.AllowedArtifacts) {
			http.Error(w, "unknown artifact type", http.StatusNotFound)
			return
		}

		// Logs carry a variable per-format schema, so they are landed as
		// immutable parquet files (queried later with union_by_name) instead of
		// being inserted into the fixed metrics hot catalog.
		if isLogArtifact(artifact) {
			//nolint:contextcheck // engine.LandLogWindow manages its own DB context internally
			landLogWindow(w, r, cfg, eng, logger, ns, artifact)
			return
		}

		body := http.MaxBytesReader(w, r.Body, cfg.MaxBodyBytes)
		//nolint:contextcheck // engine.Ingest manages its own DB context internally
		n, err := eng.Ingest(ns, body)
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				http.Error(w, "window too large", http.StatusRequestEntityTooLarge)
				return
			}
			logger.Error("ingest failed", "ns", ns, "artifact", artifact, "err", err)
			http.Error(w, "ingest failed", http.StatusInternalServerError)
			return
		}
		if n == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		logger.Info("ingested", "ns", ns, "artifact", artifact, "rows", n)
		w.WriteHeader(http.StatusNoContent)
	})
}

// isLogArtifact reports whether an artifact belongs to the logs-* family and so
// takes the land-as-file path instead of the metrics hot-catalog insert.
func isLogArtifact(artifact string) bool {
	return strings.HasPrefix(artifact, "logs-")
}

// landLogWindow persists a logs-* window as a file and writes the ingest
// response (204 on success or empty no-op; 413 when the body is too large).
func landLogWindow(w http.ResponseWriter, r *http.Request, cfg *Config, eng *engine.Engine, logger *slog.Logger, ns, artifact string) {
	body := http.MaxBytesReader(w, r.Body, cfg.MaxBodyBytes)
	n, err := eng.LandLogWindow(ns, artifact, body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "window too large", http.StatusRequestEntityTooLarge)
			return
		}
		logger.Error("log ingest failed", "ns", ns, "artifact", artifact, "err", err)
		http.Error(w, "ingest failed", http.StatusInternalServerError)
		return
	}
	if n == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	logger.Info("landed log window", "ns", ns, "artifact", artifact, "bytes", n)
	w.WriteHeader(http.StatusNoContent)
}
