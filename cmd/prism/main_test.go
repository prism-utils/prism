package main

import (
	"bytes"
	"io"
	"testing"

	"github.com/prism-utils/prism/internal/version"
)

func TestVersionOutput(t *testing.T) {
	var buf bytes.Buffer
	if err := writeVersion(&buf); err != nil {
		t.Fatalf("writeVersion: %v", err)
	}
	got, err := io.ReadAll(&buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := "prism " + version.Version + "\n"
	if string(got) != want {
		t.Fatalf("version output = %q, want %q", got, want)
	}
}
