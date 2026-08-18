package layout

import (
	"os"
	"path/filepath"
	"testing"
)

func TestColdEnabled(t *testing.T) {
	if ColdEnabled("") || ColdEnabled("  ") {
		t.Fatal("empty cold dir must disable the second root")
	}
	if !ColdEnabled("/data-cold") {
		t.Fatal("non-empty cold dir must enable the second root")
	}
}

func TestResolveRelPrefersHotThenCold(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-test"
	rel := filepath.Join("tiers", "L1", "seg.parquet")
	if err := os.MkdirAll(filepath.Join(cold, tenant, "tiers", "L1"), 0o750); err != nil {
		t.Fatal(err)
	}
	coldFile := filepath.Join(cold, tenant, rel)
	if err := os.WriteFile(coldFile, []byte("cold"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, ok := ResolveRel(hot, cold, tenant, "tiers/L1/seg.parquet")
	if !ok || got != coldFile {
		t.Fatalf("got %q ok=%v, want cold file", got, ok)
	}
	if err := os.MkdirAll(filepath.Join(hot, tenant, "tiers", "L1"), 0o750); err != nil {
		t.Fatal(err)
	}
	hotFile := filepath.Join(hot, tenant, rel)
	if err := os.WriteFile(hotFile, []byte("hot"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, ok = ResolveRel(hot, cold, tenant, "tiers/L1/seg.parquet")
	if !ok || got != hotFile {
		t.Fatalf("got %q ok=%v, want hot file when both exist", got, ok)
	}
}

func TestPathUnderAny(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	inside := filepath.Join(cold, "user-a", "tiers", "L1", "a.parquet")
	if !PathUnderAny([]string{hot, cold}, inside) {
		t.Fatal("path under cold root must be allowed")
	}
	if PathUnderAny([]string{hot, cold}, filepath.Join(t.TempDir(), "x")) {
		t.Fatal("path outside both roots must be rejected")
	}
}
