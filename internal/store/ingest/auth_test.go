package ingest_test

import (
	"testing"

	"github.com/elk-utilities/prism/internal/store/ingest"
)

func TestBearerEquals(t *testing.T) {
	cases := []struct {
		header, token string
		want          bool
	}{
		{"Bearer abc", "abc", true},
		{"Bearer  abc ", "abc", true},
		{"bearer abc", "abc", false},
		{"abc", "abc", false},
		{"Bearer abc", "xyz", false},
	}
	for _, c := range cases {
		if got := ingest.BearerEquals(c.header, c.token); got != c.want {
			t.Fatalf("BearerEquals(%q,%q)=%v want %v", c.header, c.token, got, c.want)
		}
	}
}

func TestParseAuthMode(t *testing.T) {
	cases := []struct {
		in      string
		want    ingest.AuthMode
		wantErr bool
	}{
		{"none", ingest.AuthNone, false},
		{"bearer", ingest.AuthBearer, false},
		{"mtls", ingest.AuthMTLS, false},
		{"trusted-header", ingest.AuthTrustedHeader, false},
		{"invalid", "", true},
	}
	for _, c := range cases {
		got, err := ingest.ParseAuthMode(c.in)
		if c.wantErr {
			if err == nil {
				t.Fatalf("ParseAuthMode(%q) expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseAuthMode(%q): %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("ParseAuthMode(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
