package admin

import (
	"encoding/json"
	"net/http"

	"github.com/prism-utils/prism/internal/store/queue"
)

// QueueRoutePattern returns the ServeMux pattern for the live queue snapshot.
func QueueRoutePattern() string {
	return "GET /admin/queue"
}

// QueueHandler serves GET /admin/queue with the read limiter's caps and live
// occupancy. A nil limiter reports a disabled queue with zero counters, which is
// the honest answer for a process that gates no reads of its own.
func QueueHandler(limiter *queue.Limiter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(limiter.Snapshot())
	})
}
