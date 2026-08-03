package config

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDurationMarshalRoundTrip(t *testing.T) {
	for _, d := range []Duration{0, Duration(2 * time.Second), Duration(90 * time.Second)} {
		b, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("marshal %v: %v", time.Duration(d), err)
		}
		var back Duration
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		if back != d {
			t.Fatalf("roundtrip %s = %v, want %v", b, time.Duration(back), time.Duration(d))
		}
	}
	// A non-zero duration must serialize as a quoted unit string, not a bare int
	// (a bare non-zero number is rejected on reload).
	b, _ := json.Marshal(Duration(2 * time.Second))
	if string(b) != `"2s"` {
		t.Fatalf("marshal 2s = %s, want \"2s\"", b)
	}
}

func TestByteSizeMarshalRoundTrip(t *testing.T) {
	for _, s := range []ByteSize{0, ByteSize(12 * 1024 * 1024), ByteSize(1)} {
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("marshal %d: %v", int64(s), err)
		}
		var back ByteSize
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		if back != s {
			t.Fatalf("roundtrip %s = %d, want %d", b, int64(back), int64(s))
		}
	}
}
