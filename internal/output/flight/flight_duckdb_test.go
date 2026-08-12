package flight

import (
	"testing"

	"github.com/prism-utils/prism/internal/data"
	"github.com/prism-utils/prism/internal/duckdbfile"
)

func TestDescriptorPath_DuckDBFormatMeta(t *testing.T) {
	path := descriptorPath(&data.BlockMeta{Pipeline: "metrics", Branch: "raw"})
	// Arrow path stays four segments; duckdb framing adds format via metadata helper.
	if len(path) != 4 {
		t.Fatalf("base path len = %d", len(path))
	}
	duckPath := descriptorPathDuckDB(&data.BlockMeta{Pipeline: "metrics", Branch: "raw"})
	if !duckdbfile.FormatFromFlightMeta(nil, duckPath) {
		t.Fatalf("duck path %v missing format=duckdb", duckPath)
	}
	if duckdbfile.FormatFromFlightMeta(nil, path) {
		t.Fatal("arrow path must not carry format=duckdb")
	}
}
