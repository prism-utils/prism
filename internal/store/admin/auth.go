package admin

import (
	"net/http"

	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
)

// WithBearerAuth wraps a handler with optional static bearer auth.
func WithBearerAuth(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !storeingest.BearerEquals(r.Header.Get("Authorization"), token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
