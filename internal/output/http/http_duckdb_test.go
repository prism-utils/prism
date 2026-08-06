package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/duckdbfile"
)

func TestConsume_DuckDBContentTypeDefault(t *testing.T) {
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := fastRetry(srv.URL)
	cfg.ContentType = "" // unset → duckdb default when Format=duckdb
	out := newOutput(t, cfg)
	body := make([]byte, 16)
	copy(body[duckdbfile.MagicOffset:], duckdbfile.Magic)
	block := data.EncodedBlock{Format: "duckdb", Bytes: body, Rows: 1}
	if err := out.Consume(context.Background(), block); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if gotCT != duckdbfile.ContentType {
		t.Fatalf("content-type = %q, want %q", gotCT, duckdbfile.ContentType)
	}
}

func TestConsume_DuckDBContentTypeOverride(t *testing.T) {
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := fastRetry(srv.URL)
	cfg.ContentType = "application/octet-stream"
	out := newOutput(t, cfg)
	body := make([]byte, 16)
	copy(body[duckdbfile.MagicOffset:], duckdbfile.Magic)
	block := data.EncodedBlock{Format: "duckdb", Bytes: body, Rows: 1}
	if err := out.Consume(context.Background(), block); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if gotCT != "application/octet-stream" {
		t.Fatalf("override content-type = %q", gotCT)
	}
}
