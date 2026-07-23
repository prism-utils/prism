// Package tenant validates tenant namespace and artifact-type path segments.
package tenant

import "regexp"

var tenantPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

var artifactPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{1,63}$`)

// TenantAllowed reports whether a tenant namespace segment is well-formed.
func TenantAllowed(ns string) bool {
	return tenantPattern.MatchString(ns)
}

// ArtifactAllowed reports whether an artifact type is well-formed and listed.
func ArtifactAllowed(artifact string, allowed []string) bool {
	if !artifactPattern.MatchString(artifact) {
		return false
	}
	for _, a := range allowed {
		if a == artifact {
			return true
		}
	}
	return false
}
