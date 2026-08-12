package cluster

import (
	"fmt"
	"strings"

	storeingest "github.com/prism-utils/prism/internal/store/ingest"
)

// ParseOwnedTenants builds the owned-tenant set from CLIENT_TENANTS (comma-separated).
func ParseOwnedTenants(env string) (map[string]struct{}, error) {
	env = strings.TrimSpace(env)
	if env == "" {
		return nil, fmt.Errorf("CLIENT_TENANTS: required")
	}

	out := make(map[string]struct{})
	for _, raw := range strings.Split(env, ",") {
		tenant := strings.TrimSpace(raw)
		if tenant == "" {
			continue
		}
		if !storeingest.ValidateTenant(tenant) {
			return nil, fmt.Errorf("CLIENT_TENANTS: invalid tenant %q", tenant)
		}
		out[tenant] = struct{}{}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("CLIENT_TENANTS: at least one tenant required")
	}
	return out, nil
}
