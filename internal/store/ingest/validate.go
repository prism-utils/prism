package ingest

import (
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
)

// ValidateTenant reports whether a tenant namespace is allowed.
func ValidateTenant(ns string) bool {
	return storetenant.TenantAllowed(ns)
}

// ValidateArtifact reports whether an artifact type is well-formed and listed.
func ValidateArtifact(artifact string, allowed []string) bool {
	return storetenant.ArtifactAllowed(artifact, allowed)
}
