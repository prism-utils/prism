package query

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/elk-utilities/prism/internal/store/engine"
)

// Config holds query HTTP settings.
type Config struct {
	DataDir     string
	RoutePrefix string
	ExposeSQL   bool
}

// QueryRoutePattern returns the ServeMux pattern for the query GET route.
func QueryRoutePattern(prefix string) string {
	return "GET /{ns}/query"
}

// Handler serves GET query requests under the engine read lock.
func Handler(cfg *Config, eng *engine.Engine, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, fmt.Sprintf("not implemented: %s", cfg.DataDir), http.StatusNotImplemented)
	})
}
