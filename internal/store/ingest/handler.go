package ingest

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/prism-utils/prism/internal/duckdbfile"
	"github.com/prism-utils/prism/internal/store/engine"
	storetenant "github.com/prism-utils/prism/internal/store/tenant"
)

// statusClientClosed is the nginx/Cloudflare convention for "client closed
// the connection before the server finished" (not an RFC status).
const statusClientClosed = 499

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
		// immutable files (queried later with union_by_name) instead of
		// being inserted into the fixed metrics hot catalog.
		if isLogArtifact(artifact) {
			//nolint:contextcheck // engine.LandLogWindow manages its own DB context internally
			landLogWindow(w, r, cfg, eng, logger, ns, artifact)
			return
		}

		body := http.MaxBytesReader(w, r.Body, cfg.MaxBodyBytes)
		isDuckDB, stream, err := classifyMetricsBody(r.Header.Get("Content-Type"), body)
		if err != nil {
			writeIngestError(w, logger, ns, artifact, "ingest read failed", err)
			return
		}

		var n int64
		if isDuckDB {
			//nolint:contextcheck // engine owns DB context for ingest/flush
			n, err = eng.IngestDuckDB(ns, stream)
		} else {
			//nolint:contextcheck // engine owns DB context for ingest/flush
			n, err = eng.Ingest(ns, stream)
		}
		if err != nil {
			writeIngestError(w, logger, ns, artifact, "ingest failed", err)
			return
		}
		if n == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		logger.Debug("ingested", "ns", ns, "artifact", artifact, "rows", n)
		w.WriteHeader(http.StatusNoContent)
	})
}

// isLogArtifact reports whether an artifact belongs to the logs-* family and so
// takes the land-as-file path instead of the metrics hot-catalog insert.
func isLogArtifact(artifact string) bool {
	return strings.HasPrefix(artifact, "logs-")
}

// classifyMetricsBody decides duckdb vs parquet from Content-Type, peeking only
// MagicPeek bytes when CT is empty/octet-stream (magic sniff). The returned
// reader replays any peeked prefix so the engine still sees the full body.
func classifyMetricsBody(contentType string, body io.Reader) (isDuckDB bool, stream io.Reader, err error) {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	switch ct {
	case duckdbfile.ContentType:
		return true, body, nil
	case "", "application/octet-stream":
		peek := make([]byte, duckdbfile.MagicPeek)
		n, readErr := io.ReadFull(body, peek)
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			peek = peek[:n]
			readErr = nil
		}
		if readErr != nil {
			return false, nil, readErr
		}
		return duckdbfile.HasMagic(peek), io.MultiReader(bytes.NewReader(peek), body), nil
	default:
		return false, body, nil
	}
}

// landLogWindow persists a logs-* window as a file and writes the ingest
// response (204 on success or empty no-op; 413 when the body is too large).
func landLogWindow(w http.ResponseWriter, r *http.Request, cfg *Config, eng *engine.Engine, logger *slog.Logger, ns, artifact string) {
	body := http.MaxBytesReader(w, r.Body, cfg.MaxBodyBytes)
	n, err := eng.LandLogWindow(ns, artifact, body)
	if err != nil {
		writeIngestError(w, logger, ns, artifact, "log ingest failed", err)
		return
	}
	if n == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	logger.Debug("landed log window", "ns", ns, "artifact", artifact, "bytes", n)
	w.WriteHeader(http.StatusNoContent)
}

func writeIngestError(w http.ResponseWriter, logger *slog.Logger, ns, artifact, msg string, err error) {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		http.Error(w, "window too large", http.StatusRequestEntityTooLarge)
		return
	}
	if errors.Is(err, engine.ErrIncompatibleDuckDBStorage) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if isClientAbort(err) {
		logger.Warn(msg, "ns", ns, "artifact", artifact, "err", err, "status", statusClientClosed)
		http.Error(w, "client closed", statusClientClosed)
		return
	}
	logger.Error(msg, "ns", ns, "artifact", artifact, "err", err)
	http.Error(w, "ingest failed", http.StatusInternalServerError)
}

func isClientAbort(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, engine.ErrClientAbort) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	return false
}
