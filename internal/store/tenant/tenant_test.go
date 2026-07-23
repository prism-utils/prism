package tenant

import (
	"strings"
	"testing"
)

func TestTenantAllowed(t *testing.T) {
	long63 := "a" + strings.Repeat("b", 62)
	long64 := "a" + strings.Repeat("b", 63)
	tests := []struct {
		name string
		ns   string
		want bool
	}{
		{name: "empty", ns: "", want: false},
		{name: "single char valid", ns: "a", want: true},
		{name: "leading dot", ns: ".abc", want: false},
		{name: "leading dash", ns: "-abc", want: false},
		{name: "max length 63", ns: long63, want: true},
		{name: "too long 64", ns: long64, want: false},
		{name: "uppercase", ns: "ABC", want: false},
		{name: "path traversal", ns: "../x", want: false},
		{name: "path separator", ns: "a/b", want: false},
		{name: "valid tenant", ns: "user-6f3a9c2b-apps", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TenantAllowed(tt.ns); got != tt.want {
				t.Fatalf("TenantAllowed(%q) = %v, want %v", tt.ns, got, tt.want)
			}
		})
	}
}

func TestArtifactAllowed(t *testing.T) {
	allowed := []string{"metrics-raw"}
	tests := []struct {
		name     string
		artifact string
		allowed  []string
		want     bool
	}{
		{name: "metrics-raw allowed", artifact: "metrics-raw", allowed: allowed, want: true},
		{name: "metrics-raw not in list", artifact: "metrics-raw", allowed: []string{"logs-raw"}, want: false},
		{name: "malformed artifact", artifact: "../escape", allowed: allowed, want: false},
		{name: "empty artifact", artifact: "", allowed: allowed, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ArtifactAllowed(tt.artifact, tt.allowed); got != tt.want {
				t.Fatalf("ArtifactAllowed(%q, %v) = %v, want %v", tt.artifact, tt.allowed, got, tt.want)
			}
		})
	}
}
