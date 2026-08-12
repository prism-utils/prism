package prometheus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
)

const exposition = `# HELP up target up
# TYPE up gauge
up{job="x"} 1
go_goroutines 12
`

func newInput(t *testing.T, targets []string, interval string) *Input {
	t.Helper()
	f := NewFactory()
	cfg := f.DefaultConfig().(*Config)
	cfg.Targets = targets
	cfg.Interval = interval
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	in, err := f.Create(cfg, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return in.(*Input)
}

func TestScrape_EmitsExpositionLines(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(exposition))
	}))
	defer srv.Close()

	in := newInput(t, []string{srv.URL}, "1h") // one immediate scrape; ticker won't fire
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := in.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var batch data.RawBatch
	select {
	case batch = <-in.Batches():
	case <-time.After(3 * time.Second):
		t.Fatal("no batch scraped within timeout")
	}
	if batch.Source != srv.URL {
		t.Fatalf("source = %q, want %q", batch.Source, srv.URL)
	}
	// 4 comments/samples from exposition + 1 synthetic `up 1` from the scraper.
	if len(batch.Records) != 5 {
		t.Fatalf("records = %d, want 5", len(batch.Records))
	}
	last := string(batch.Records[len(batch.Records)-1])
	if last != "up 1" {
		t.Fatalf("last record = %q, want %q", last, "up 1")
	}
	cancel()
	// Drain until the loop closes the channel so no goroutine lingers.
	for range in.Batches() {
	}
}

func TestValidate_Errors(t *testing.T) {
	if err := (&Config{}).Validate(); err == nil {
		t.Fatal("no targets should be invalid")
	}
	if err := (&Config{Targets: []string{"http://x"}, Interval: "nope"}).Validate(); err == nil {
		t.Fatal("bad interval should be invalid")
	}
	if err := (&Config{Targets: []string{"http://x"}, Labels: map[string]string{"": "v"}}).Validate(); err == nil {
		t.Fatal("empty label name should be invalid")
	}
}

func TestScrape_AttachesTargetLabels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(exposition))
	}))
	defer srv.Close()

	f := NewFactory()
	cfg := f.DefaultConfig().(*Config)
	cfg.Targets = []string{srv.URL}
	cfg.Interval = "1h"
	cfg.Labels = map[string]string{"job": "demo"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	created, err := f.Create(cfg, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	in := created.(*Input)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := in.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	var batch data.RawBatch
	select {
	case batch = <-in.Batches():
	case <-time.After(3 * time.Second):
		t.Fatal("no batch scraped within timeout")
	}
	if batch.Labels["job"] != "demo" {
		t.Fatalf("job label = %q, want demo", batch.Labels["job"])
	}
	// instance is derived from the target host:port (httptest is 127.0.0.1:port).
	if got := batch.Labels["instance"]; got == "" || got == srv.URL {
		t.Fatalf("instance label = %q, want host:port", got)
	}
	cancel()
	for range in.Batches() {
	}
}

func TestInstanceFromTarget(t *testing.T) {
	cases := map[string]string{
		"http://demo-clickhouse:9363/metrics": "demo-clickhouse:9363",
		"https://host/metrics":                "host",
		"://bad":                              "",
	}
	for in, want := range cases {
		if got := instanceFromTarget(in); got != want {
			t.Fatalf("instanceFromTarget(%q) = %q, want %q", in, got, want)
		}
	}
}
