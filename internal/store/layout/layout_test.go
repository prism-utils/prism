package layout

import (
	"strings"
	"testing"
	"time"
)

func TestSegmentNameUniqueForSameInstant(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		name := SegmentName(now)
		if !strings.HasSuffix(name, ".parquet") {
			t.Fatalf("want .parquet suffix, got %q", name)
		}
		if _, dup := seen[name]; dup {
			t.Fatalf("collision at same instant: %q", name)
		}
		seen[name] = struct{}{}
	}
}

func TestSegmentNameOrdersByInstant(t *testing.T) {
	earlier := SegmentName(time.Unix(1700000000, 0).UTC())
	later := SegmentName(time.Unix(1700000001, 0).UTC())
	if earlier >= later {
		t.Fatalf("want lexicographic order by instant, got %q >= %q", earlier, later)
	}
}
