//go:build cgo

package store

import (
	"slices"
	"testing"
)

func TestServerEnvHotOnly(t *testing.T) {
	t.Parallel()
	d := &cgoDriver{cfg: Config{HotOnly: true, DataDir: t.TempDir(), Tenant: "bench-tenant"}}
	env := d.serverEnv()
	if !slices.Contains(env, "QUERY_HOT_ONLY=true") {
		t.Fatalf("serverEnv=%v missing QUERY_HOT_ONLY=true", env)
	}
}

func TestServerEnvHotOnlyDefaultOff(t *testing.T) {
	t.Parallel()
	d := &cgoDriver{cfg: Config{DataDir: t.TempDir(), Tenant: "bench-tenant"}}
	for _, e := range d.serverEnv() {
		if e == "QUERY_HOT_ONLY=true" {
			t.Fatal("QUERY_HOT_ONLY must be absent when HotOnly is false")
		}
	}
}
