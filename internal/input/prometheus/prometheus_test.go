package prometheus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
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
	// 4 non-blank lines (2 comments + 2 samples); parser drops comments later.
	if len(batch.Records) != 4 {
		t.Fatalf("records = %d, want 4", len(batch.Records))
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
}
