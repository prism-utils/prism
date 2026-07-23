package cluster

import (
	"net/http"

	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
)

// OwnedTenantGuard rejects query requests whose tenant is not in the owned set.
func OwnedTenantGuard(owned map[string]struct{}, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")
		if !storeingest.ValidateTenant(ns) {
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}
		if _, ok := owned[ns]; !ok {
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}
