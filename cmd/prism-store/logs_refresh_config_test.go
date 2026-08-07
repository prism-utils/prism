package main

import (
	"testing"
	"time"
)

func TestLoadConfigLogsRefreshDefaults(t *testing.T) {
	clearStoreEnv(t)
	cfg := loadConfig()
	if cfg.logsRefreshInterval != time.Minute {
		t.Fatalf("logsRefreshInterval = %v, want 1m", cfg.logsRefreshInterval)
	}
	if cfg.logsRefreshMaxActs != 8 {
		t.Fatalf("logsRefreshMaxActs = %d, want 8", cfg.logsRefreshMaxActs)
	}
}

func TestLoadConfigLogsRefreshFromEnv(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("LOGS_REFRESH_INTERVAL", "15")
	t.Setenv("LOGS_REFRESH_MAX_ACTIONS", "3")
	cfg := loadConfig()
	if cfg.logsRefreshInterval != 15*time.Second {
		t.Fatalf("logsRefreshInterval = %v, want 15s", cfg.logsRefreshInterval)
	}
	if cfg.logsRefreshMaxActs != 3 {
		t.Fatalf("logsRefreshMaxActs = %d, want 3", cfg.logsRefreshMaxActs)
	}
}

func TestLoadConfigLogsRefreshZeroDisablesAgeTrigger(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("LOGS_REFRESH_INTERVAL", "0")
	cfg := loadConfig()
	if cfg.logsRefreshInterval != 0 {
		t.Fatalf("logsRefreshInterval = %v, want 0 (age trigger off)", cfg.logsRefreshInterval)
	}
}

func TestLoadConfigLogsRefreshRejectsGarbage(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("LOGS_REFRESH_INTERVAL", "soon")
	t.Setenv("LOGS_REFRESH_MAX_ACTIONS", "-4")
	cfg := loadConfig()
	if cfg.logsRefreshInterval != time.Minute {
		t.Fatalf("logsRefreshInterval = %v, want the 1m default on unparsable input", cfg.logsRefreshInterval)
	}
	if cfg.logsRefreshMaxActs != 8 {
		t.Fatalf("logsRefreshMaxActs = %d, want the default on a negative value", cfg.logsRefreshMaxActs)
	}
}
