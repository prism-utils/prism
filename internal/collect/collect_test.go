package collect

import (
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

func TestTimeFromNano(t *testing.T) {
	if got := timeFromNano("0"); !got.IsZero() {
		t.Fatalf("nano 0 should be zero time, got %v", got)
	}
	if got := timeFromNano("not-a-number"); !got.IsZero() {
		t.Fatalf("garbage should be zero time, got %v", got)
	}
	want := time.Date(2026, 7, 4, 1, 2, 3, 0, time.UTC)
	if got := timeFromNano(nanoStr(want)); !got.Equal(want) {
		t.Fatalf("nano decode = %v, want %v", got, want)
	}
}

func TestMetaFromDescriptor(t *testing.T) {
	// Nil/short descriptors fall back to a sane default rather than panicking.
	def := metaFromDescriptor(nil)
	if def.Pipeline != "flight" || def.Branch != "raw" {
		t.Fatalf("default meta = %+v", def)
	}
	short := metaFromDescriptor(&flight.FlightDescriptor{Path: []string{"only", "two"}})
	if short.Pipeline != "flight" {
		t.Fatalf("short path should fall back, got %+v", short)
	}

	start := time.Date(2026, 7, 4, 1, 2, 3, 0, time.UTC)
	end := start.Add(time.Second)
	full := metaFromDescriptor(&flight.FlightDescriptor{
		Path: []string{"metrics", "wire", nanoStr(start), nanoStr(end)},
	})
	if full.Pipeline != "metrics" || full.Branch != "wire" {
		t.Fatalf("provenance lost: %+v", full)
	}
	if !full.Window.Start.Equal(start) || !full.Window.End.Equal(end) {
		t.Fatalf("window decode = %+v", full.Window)
	}
}

func nanoStr(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}
