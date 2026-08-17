package config_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/config"
)

func TestDuration_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{`"30s"`, 30 * time.Second, false},
		{`"1m30s"`, 90 * time.Second, false},
		{`0`, 0, false},
		{`1500000000`, 0, true}, // bare numbers are ambiguous units: rejected
		{`"nope"`, 0, true},
		{`""`, 0, true},
	}
	for _, c := range cases {
		var d config.Duration
		err := json.Unmarshal([]byte(c.in), &d)
		if c.wantErr {
			if err == nil {
				t.Errorf("Duration(%s): expected error, got %v", c.in, time.Duration(d))
			}
			continue
		}
		if err != nil {
			t.Errorf("Duration(%s): unexpected error: %v", c.in, err)
			continue
		}
		if time.Duration(d) != c.want {
			t.Errorf("Duration(%s) = %v, want %v", c.in, time.Duration(d), c.want)
		}
	}
}

func TestByteSize_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{`"12MiB"`, 12 * mib, false},
		{`"1KiB"`, kib, false},
		{`"2GiB"`, 2 * gib, false},
		{`"1000"`, 1000, false},     // plain bytes as string
		{`4096`, 4096, false},       // plain bytes as number
		{`"1MB"`, 1_000_000, false}, // decimal SI
		{`"nope"`, 0, true},
		{`"-5MiB"`, 0, true},
	}
	for _, c := range cases {
		var b config.ByteSize
		err := json.Unmarshal([]byte(c.in), &b)
		if c.wantErr {
			if err == nil {
				t.Errorf("ByteSize(%s): expected error, got %d", c.in, int64(b))
			}
			continue
		}
		if err != nil {
			t.Errorf("ByteSize(%s): unexpected error: %v", c.in, err)
			continue
		}
		if int64(b) != c.want {
			t.Errorf("ByteSize(%s) = %d, want %d", c.in, int64(b), c.want)
		}
	}
}

func TestParseByteSizeOrZero(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"off", 0},
		{"1638MB", 1638_000_000},
		{"1433MiB", 1433 << 20},
		{"512", 512},
	}
	for _, c := range cases {
		if got := config.ParseByteSizeOrZero(c.in); got != c.want {
			t.Errorf("ParseByteSizeOrZero(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
