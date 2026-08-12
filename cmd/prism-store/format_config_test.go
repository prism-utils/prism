package main

import (
	"strings"
	"testing"

	"github.com/prism-utils/prism/internal/store/segformat"
)

func TestLoadConfigSegmentFormatsDefaultParquet(t *testing.T) {
	clearStoreEnv(t)
	cfg := loadConfig()
	if cfg.hotSegmentFormat != string(segformat.Parquet) {
		t.Fatalf("hotSegmentFormat=%q want parquet", cfg.hotSegmentFormat)
	}
	if cfg.mergeSegmentFormat != string(segformat.Parquet) {
		t.Fatalf("mergeSegmentFormat=%q want parquet", cfg.mergeSegmentFormat)
	}
	if cfg.duckdbStorageVersion != segformat.DefaultStorageVersion {
		t.Fatalf("duckdbStorageVersion=%q want %q", cfg.duckdbStorageVersion, segformat.DefaultStorageVersion)
	}
}

func TestLoadConfigSegmentFormatsFromEnv(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("HOT_SEGMENT_FORMAT", "duckdb")
	t.Setenv("MERGE_SEGMENT_FORMAT", "DUCKDB")
	t.Setenv("DUCKDB_STORAGE_VERSION", "v1.2.0")
	cfg := loadConfig()
	if err := cfg.validateSegmentFormats(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.hotSegmentFormat != "duckdb" {
		t.Fatalf("hotSegmentFormat=%q", cfg.hotSegmentFormat)
	}
	if cfg.mergeSegmentFormat != "duckdb" {
		t.Fatalf("mergeSegmentFormat=%q", cfg.mergeSegmentFormat)
	}
	if cfg.duckdbStorageVersion != "v1.2.0" {
		t.Fatalf("duckdbStorageVersion=%q", cfg.duckdbStorageVersion)
	}
}

func TestLoadConfigRejectsInvalidSegmentFormats(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("HOT_SEGMENT_FORMAT", "orc")
	cfg := loadConfig()
	err := cfg.validateSegmentFormats()
	if err == nil || !strings.Contains(err.Error(), "HOT_SEGMENT_FORMAT") {
		t.Fatalf("want HOT_SEGMENT_FORMAT error, got %v", err)
	}

	clearStoreEnv(t)
	t.Setenv("MERGE_SEGMENT_FORMAT", "json")
	cfg = loadConfig()
	err = cfg.validateSegmentFormats()
	if err == nil || !strings.Contains(err.Error(), "MERGE_SEGMENT_FORMAT") {
		t.Fatalf("want MERGE_SEGMENT_FORMAT error, got %v", err)
	}
}
