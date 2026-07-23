package ingest

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/elk-utilities/prism/internal/store/engine"
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
			http.Error(w, "unknown tenant", http.StatusNotFound)
			return
		}
		if !ValidateArtifact(artifact, cfg.AllowedArtifacts) {
			http.Error(w, "unknown artifact type", http.StatusNotFound)
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
