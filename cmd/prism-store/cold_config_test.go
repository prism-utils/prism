package main

import (
	"testing"
	"time"
)

func TestLoadConfigColdDirDefaultOff(t *testing.T) {
	clearStoreEnv(t)
	cfg := loadConfig()
	if cfg.coldDir != "" {
		t.Fatalf("coldDir = %q, want empty", cfg.coldDir)
	}
	if cfg.coldAfter != 12*time.Hour {
		t.Fatalf("coldAfter = %v, want 12h", cfg.coldAfter)
	}
}

func TestLoadConfigColdDirFromEnv(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("COLD_DATA_DIR", "/data-cold")
	t.Setenv("COLD_AFTER", "6h")
	cfg := loadConfig()
	if cfg.coldDir != "/data-cold" {
		t.Fatalf("coldDir = %q, want /data-cold", cfg.coldDir)
	}
	if cfg.coldAfter != 6*time.Hour {
		t.Fatalf("coldAfter = %v, want 6h", cfg.coldAfter)
	}
}

func TestLoadConfigColdAfterRejectsGarbageAndZero(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("COLD_AFTER", "soon")
	cfg := loadConfig()
	if cfg.coldAfter != 12*time.Hour {
		t.Fatalf("coldAfter = %v, want 12h on unparsable input", cfg.coldAfter)
	}
	t.Setenv("COLD_AFTER", "0")
	cfg = loadConfig()
	if cfg.coldAfter != 12*time.Hour {
		t.Fatalf("coldAfter = %v, want 12h on zero", cfg.coldAfter)
	}
}
