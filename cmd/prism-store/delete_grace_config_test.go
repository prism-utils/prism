package main

import (
	"testing"
	"time"
)

func TestLoadConfigDeleteGraceDefault(t *testing.T) {
	clearStoreEnv(t)
	cfg := loadConfig()
	if cfg.deleteGrace != 120*time.Second {
		t.Fatalf("deleteGrace = %v, want 2m", cfg.deleteGrace)
	}
}

func TestLoadConfigDeleteGraceFromEnv(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("LOGS_DELETE_GRACE_SECONDS", "45")
	cfg := loadConfig()
	if cfg.deleteGrace != 45*time.Second {
		t.Fatalf("deleteGrace = %v, want 45s", cfg.deleteGrace)
	}
}

// Zero is the documented escape hatch: delete compacted sources on the spot,
// which is what an operator who needs the disk back right now reaches for.
func TestLoadConfigDeleteGraceZeroDisablesHold(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("LOGS_DELETE_GRACE_SECONDS", "0")
	cfg := loadConfig()
	if cfg.deleteGrace != 0 {
		t.Fatalf("deleteGrace = %v, want 0 (immediate delete)", cfg.deleteGrace)
	}
}

func TestLoadConfigDeleteGraceRejectsGarbage(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("LOGS_DELETE_GRACE_SECONDS", "soon")
	cfg := loadConfig()
	if cfg.deleteGrace != 120*time.Second {
		t.Fatalf("deleteGrace = %v, want the 2m default on unparsable input", cfg.deleteGrace)
	}
}

func TestLoadConfigDeleteGraceRejectsNegative(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("LOGS_DELETE_GRACE_SECONDS", "-30")
	cfg := loadConfig()
	if cfg.deleteGrace != 120*time.Second {
		t.Fatalf("deleteGrace = %v, want the 2m default on a negative value", cfg.deleteGrace)
	}
}
