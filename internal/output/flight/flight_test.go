package flight

import (
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/data"
)

func TestConfig_Validate(t *testing.T) {
	if err := (&Config{}).Validate(); err == nil {
		t.Fatal("empty addr should be invalid")
	}
	if err := (&Config{Addr: "localhost:8815"}).Validate(); err != nil {
		t.Fatalf("valid addr rejected: %v", err)
	}
}

func TestDescriptorPath_EncodesProvenance(t *testing.T) {
	start := time.Date(2026, 7, 4, 1, 2, 3, 0, time.UTC)
	end := start.Add(time.Second)
	got := descriptorPath(&data.BlockMeta{
		Pipeline: "logs",
		Branch:   "template",
		Window:   data.TimeWindow{Start: start, End: end},
	})
	if len(got) != 4 {
		t.Fatalf("path len = %d, want 4", len(got))
	}
	if got[0] != "logs" || got[1] != "template" {
		t.Fatalf("provenance = %v", got[:2])
	}
	if got[2] != nano(start) || got[3] != nano(end) {
		t.Fatalf("window nanos = %v", got[2:])
	}
	if got[2] >= got[3] {
		t.Fatalf("start %s not before end %s", got[2], got[3])
	}
}

func TestDescriptorPath_NilAndZero(t *testing.T) {
	nilPath := descriptorPath(nil)
	if len(nilPath) != 4 || nilPath[0] != "unknown" || nilPath[2] != "0" {
		t.Fatalf("nil meta path = %v", nilPath)
	}
	zeroPath := descriptorPath(&data.BlockMeta{})
	if zeroPath[0] != "unknown" || zeroPath[2] != "0" || zeroPath[3] != "0" {
		t.Fatalf("zero meta path = %v", zeroPath)
	}
}
