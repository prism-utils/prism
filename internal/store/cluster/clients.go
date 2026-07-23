package cluster

import (
	"fmt"
	"net/url"
	"strings"

	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
)

// ParseClients builds a tenant-to-base-URL map from CLUSTER_CLIENTS.
// Format: tenantA=http://host1:8080,tenantB=http://host2:8080
func ParseClients(env string) (map[string]*url.URL, error) {
	env = strings.TrimSpace(env)
	if env == "" {
		return nil, fmt.Errorf("CLUSTER_CLIENTS: required")
	}

	out := make(map[string]*url.URL)
	for _, entry := range strings.Split(env, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		tenant, rawURL, ok := strings.Cut(entry, "=")
		tenant = strings.TrimSpace(tenant)
		rawURL = strings.TrimSpace(rawURL)
		if !ok || rawURL == "" {
			return nil, fmt.Errorf("CLUSTER_CLIENTS: malformed entry %q", entry)
		}
		if tenant == "" {
			return nil, fmt.Errorf("CLUSTER_CLIENTS: empty tenant in entry %q", entry)
		}
		if !storeingest.ValidateTenant(tenant) {
			return nil, fmt.Errorf("CLUSTER_CLIENTS: invalid tenant %q", tenant)
		}
		if _, exists := out[tenant]; exists {
			return nil, fmt.Errorf("CLUSTER_CLIENTS: duplicate tenant %q", tenant)
		}
		u, err := url.Parse(rawURL)
		if err != nil {
			return nil, fmt.Errorf("CLUSTER_CLIENTS: tenant %q URL: %w", tenant, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("CLUSTER_CLIENTS: tenant %q URL must be http or https", tenant)
		}
		if u.Host == "" {
			return nil, fmt.Errorf("CLUSTER_CLIENTS: tenant %q URL must be absolute", tenant)
		}
		out[tenant] = u
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("CLUSTER_CLIENTS: at least one tenant required")
	}
	return out, nil
}
