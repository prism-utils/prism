package merge

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadCompactEmptyPathNoPolicies(t *testing.T) {
	t.Parallel()
	f, err := LoadCompact("")
	if err != nil {
		t.Fatalf("LoadCompact empty: %v", err)
	}
	if len(f.Policies) != 0 {
		t.Fatalf("want no policies, got %d", len(f.Policies))
	}
}

func TestLoadCompactMissingFileErrors(t *testing.T) {
	t.Parallel()
	_, err := LoadCompact(filepath.Join(t.TempDir(), "no-such.yaml"))
	if err == nil {
		t.Fatal("LoadCompact missing file: want error")
	}
}

func TestLoadCompactValidYAML(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "compact.yaml")
	body := []byte(`
compact:
  policies:
    - name: recent
      tier: 0
      olderThan: 15m
      newerThan: 1h
      maxSources: 32
      maxBytes: 256Mi
      every: 45m
      bucket: none
    - name: daily
      tier: 0
      bucket: day
      olderThan: 1d
      newerThan: 3d
      maxSources: 64
      maxBytes: 512Mi
      every: 1h
`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := LoadCompact(path)
	if err != nil {
		t.Fatalf("LoadCompact: %v", err)
	}
	if err := f.Validate(2 << 30); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(f.Policies) != 2 {
		t.Fatalf("policies = %d, want 2", len(f.Policies))
	}
	recent, ok := f.Lookup("recent")
	if !ok {
		t.Fatal("missing recent")
	}
	spec, err := recent.Spec()
	if err != nil {
		t.Fatalf("recent Spec: %v", err)
	}
	if spec.OlderThan != 15*time.Minute || spec.NewerThan != time.Hour {
		t.Fatalf("recent window = %+v", spec)
	}
	if spec.MaxSources != 32 || spec.MaxBytes != 256<<20 {
		t.Fatalf("recent caps = %+v", spec)
	}
	daily, ok := f.Lookup("daily")
	if !ok {
		t.Fatal("missing daily")
	}
	dspec, err := daily.Spec()
	if err != nil {
		t.Fatalf("daily Spec: %v", err)
	}
	if dspec.Bucket != BucketDay || dspec.OlderThan != 24*time.Hour {
		t.Fatalf("daily spec = %+v", dspec)
	}
	if daily.Interval() != time.Hour {
		t.Fatalf("daily every = %s", daily.Interval())
	}
}

func TestValidateCompactRejectsBadNamesAndCaps(t *testing.T) {
	t.Parallel()
	cases := []Policy{
		{Name: "HasCaps", MaxSources: 2, MaxBytes: "1Mi", Every: "1m"},
		{Name: "bad-dash", MaxSources: 2, MaxBytes: "1Mi", Every: "1m"},
		{Name: "", MaxSources: 2, MaxBytes: "1Mi", Every: "1m"},
		{Name: "ok", MaxSources: 1, MaxBytes: "1Mi", Every: "1m"},
		{Name: "ok", MaxSources: 2, MaxBytes: "0", Every: "1m"},
		{Name: "ok", MaxSources: 2, MaxBytes: "", Every: "1m"},
		{Name: "ok", MaxSources: 2, MaxBytes: "1Mi", Every: ""},
		{Name: "ok", MaxSources: 2, MaxBytes: "1Mi", Every: "bogus"},
		{Name: "ok", MaxSources: 2, MaxBytes: "1Mi", Every: "1m", Bucket: "week"},
	}
	for _, p := range cases {
		f := File{Policies: []Policy{p}}
		if err := f.Validate(2 << 30); err == nil {
			t.Fatalf("Validate(%+v) = nil, want error", p)
		}
	}
}

func TestValidateCompactDuplicateNames(t *testing.T) {
	t.Parallel()
	f := File{Policies: []Policy{
		{Name: "recent", MaxSources: 2, MaxBytes: "1Mi", Every: "1m"},
		{Name: "recent", MaxSources: 2, MaxBytes: "1Mi", Every: "1m"},
	}}
	err := f.Validate(2 << 30)
	if err == nil {
		t.Fatal("duplicate names must fail")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("error %q should mention duplicate", err)
	}
}

func TestValidateCompactMaxBytesExceedsSeal(t *testing.T) {
	t.Parallel()
	f := File{Policies: []Policy{
		{Name: "big", MaxSources: 2, MaxBytes: "3Gi", Every: "1m"},
	}}
	err := f.Validate(2 << 30)
	if err == nil {
		t.Fatal("maxBytes above seal budget must fail")
	}
	if !strings.Contains(err.Error(), "maxBytes") {
		t.Fatalf("error %q should name maxBytes", err)
	}
}

func TestValidateCompactNamesOffendingPath(t *testing.T) {
	t.Parallel()
	f := File{Policies: []Policy{{Name: "Bad", MaxSources: 2, MaxBytes: "1Mi", Every: "1m"}}}
	err := f.Validate(2 << 30)
	if err == nil {
		t.Fatal("want error")
	}
	if !strings.Contains(err.Error(), "policies[0].name") {
		t.Fatalf("error %q should name config path", err)
	}
}
