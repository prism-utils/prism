package duckdbfile_test

import (
	"testing"

	"github.com/elk-utilities/prism/internal/duckdbfile"
	"github.com/elk-utilities/prism/internal/store/segformat"
)

func TestHasMagic(t *testing.T) {
	b := make([]byte, 16)
	copy(b[duckdbfile.MagicOffset:], duckdbfile.Magic)
	if !duckdbfile.HasMagic(b) {
		t.Fatal("expected duckdb magic")
	}
	if duckdbfile.HasMagic([]byte("PAR1........")) {
		t.Fatal("parquet must not match duckdb magic")
	}
	if duckdbfile.HasMagic(nil) || duckdbfile.HasMagic(b[:8]) {
		t.Fatal("short buffer must not match")
	}
}

func TestDetectHTTP(t *testing.T) {
	duck := make([]byte, 16)
	copy(duck[duckdbfile.MagicOffset:], duckdbfile.Magic)
	parq := []byte("PAR1xxxxxxxx")

	cases := []struct {
		ct   string
		body []byte
		want bool
	}{
		{duckdbfile.ContentType, []byte("x"), true},
		{duckdbfile.ContentType + "; charset=binary", duck, true},
		{"application/octet-stream", duck, true},
		{"", duck, true},
		{"application/octet-stream", parq, false},
		{"", parq, false},
		{"application/vnd.apache.parquet", duck, false},
	}
	for _, tc := range cases {
		if got := duckdbfile.DetectHTTP(tc.ct, tc.body); got != tc.want {
			t.Fatalf("DetectHTTP(%q) = %v, want %v", tc.ct, got, tc.want)
		}
	}
}

func TestFormatFromFlightMeta(t *testing.T) {
	if !duckdbfile.FormatFromFlightMeta([]byte(duckdbfile.FormatMeta), nil) {
		t.Fatal("app metadata should match")
	}
	if !duckdbfile.FormatFromFlightMeta(nil, []string{"p", "b", "0", "1", duckdbfile.FormatMeta}) {
		t.Fatal("path token should match")
	}
	if duckdbfile.FormatFromFlightMeta(nil, []string{"p", "b", "0", "1"}) {
		t.Fatal("arrow path must not match")
	}
}

func TestStorageVersionMatchesSegformat(t *testing.T) {
	if duckdbfile.DefaultStorageVersion != segformat.DefaultStorageVersion {
		t.Fatalf("agent pin %q != store pin %q",
			duckdbfile.DefaultStorageVersion, segformat.DefaultStorageVersion)
	}
}
