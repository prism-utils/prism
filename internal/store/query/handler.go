package query

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
)

// Config holds query HTTP settings.
type Config struct {
	DataDir     string
	RoutePrefix string
	ExposeSQL   bool
	HotOnly     bool
}

// QueryRoutePattern returns the ServeMux pattern for the query GET route.
func QueryRoutePattern(prefix string) string {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return "GET /{ns}/query"
	}
	return "GET " + prefix + "/{ns}/query"
}

// Handler serves GET query requests under the engine read lock.
func Handler(cfg *Config, eng *engine.Engine, logger *slog.Logger) http.Handler {
	b := &Builder{DataDir: cfg.DataDir, HotOnly: cfg.HotOnly}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")
		if !storeingest.ValidateTenant(ns) {
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}

		startRaw := r.URL.Query().Get("start")
		endRaw := r.URL.Query().Get("end")
		if startRaw == "" || endRaw == "" {
			http.Error(w, "start and end required", http.StatusBadRequest)
			return
		}
		start, err := time.Parse(time.RFC3339, startRaw)
		if err != nil {
			http.Error(w, "invalid start", http.StatusBadRequest)
			return
		}
		end, err := time.Parse(time.RFC3339, endRaw)
		if err != nil {
			http.Error(w, "invalid end", http.StatusBadRequest)
			return
		}
		step := r.URL.Query().Get("step")

		if _, err := os.Stat(filepath.Join(cfg.DataDir, ns)); err != nil { //nolint:gosec // G703: ns validated by ValidateTenant before join
			if os.IsNotExist(err) {
				http.Error(w, "bad query", http.StatusBadRequest)
				return
			}
			logger.Error("query stat tenant root", "ns", ns, "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}

		var payload []byte
		ctx := r.Context()
		//nolint:contextcheck // read callback receives *sql.DB only; request ctx is captured above.
		if err := eng.WithRead(ns, func(db *sql.DB) error {
			req := &Request{
				Tenant: ns,
				Start:  start.UTC(),
				End:    end.UTC(),
				Step:   step,
			}
			sqlText, args, buildErr := b.BuildSQLWithDB(ctx, req, db)
			if buildErr != nil {
				return buildErr
			}
			rows, execErr := Execute(ctx, db, sqlText, args)
			if execErr != nil {
				return execErr
			}
			var encErr error
			payload, encErr = ToJSON(rows, cfg.ExposeSQL, sqlText)
			return encErr
		}); err != nil {
			if isBadQueryErr(err) {
				http.Error(w, "bad query", http.StatusBadRequest)
				return
			}
			logger.Error("query failed", "ns", ns, "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(payload); err != nil {
			logger.Error("query write failed", "ns", ns, "err", err)
		}
	})
}

func isBadQueryErr(err error) bool {
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && os.IsNotExist(pathErr.Err) {
		return true
	}
	return strings.Contains(err.Error(), "tenant root")
}
