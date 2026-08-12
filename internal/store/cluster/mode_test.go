package cluster_test

import (
	"errors"
	"testing"

	"github.com/prism-utils/prism/internal/store/cluster"
)

func TestParseModeEmptyDefaultStandalone(t *testing.T) {
	m, err := cluster.ParseMode("")
	if err != nil {
		t.Fatalf("ParseMode: %v", err)
	}
	if m != cluster.ModeStandalone {
		t.Fatalf("mode = %q, want standalone", m)
	}
}

func TestParseModeStandalone(t *testing.T) {
	m, err := cluster.ParseMode("standalone")
	if err != nil {
		t.Fatal(err)
	}
	if m != cluster.ModeStandalone {
		t.Fatalf("mode = %q", m)
	}
}

func TestParseModeClient(t *testing.T) {
	m, err := cluster.ParseMode("client")
	if err != nil {
		t.Fatal(err)
	}
	if m != cluster.ModeClient {
		t.Fatalf("mode = %q", m)
	}
}

func TestParseModeCluster(t *testing.T) {
	m, err := cluster.ParseMode("cluster")
	if err != nil {
		t.Fatal(err)
	}
	if m != cluster.ModeCluster {
		t.Fatalf("mode = %q", m)
	}
}

func TestParseModeInvalid(t *testing.T) {
	_, err := cluster.ParseMode("replica")
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
	if !errors.Is(err, cluster.ErrInvalidMode) {
		t.Fatalf("err = %v, want ErrInvalidMode", err)
	}
}
