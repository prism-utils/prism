package materialize

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/prism-utils/prism/internal/store/layout"
)

const seedName = "_seed.parquet"

// LiveFiles lists non-compacted materialization parquet (or duckdb) files.
func LiveFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	compacted := layout.CompactedSet(entries)
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if layout.IsCompactedMarker(name) || layout.IsCompactedMarkerTemp(name) {
			continue
		}
		if name == seedName || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".tmp") {
			continue
		}
		if _, skip := compacted[name]; skip {
			continue
		}
		switch {
		case strings.HasSuffix(name, ".parquet"), strings.HasSuffix(name, ".duckdb"):
			out = append(out, filepath.Join(dir, name))
		}
	}
	return out, nil
}

// ViewSQL is the sandbox SELECT for mat_<name>. It never mentions tiers or hot.
func ViewSQL(files []string) string {
	if len(files) == 0 {
		return "SELECT NULL WHERE 1=0"
	}
	parts := make([]string, 0, len(files))
	for _, f := range files {
		if strings.HasSuffix(f, ".duckdb") {
			continue
		}
		parts = append(parts, fmt.Sprintf("SELECT * FROM read_parquet(%s)", quotePath(f)))
	}
	if len(parts) == 0 {
		return "SELECT NULL WHERE 1=0"
	}
	return strings.Join(parts, " UNION ALL ")
}

func quotePath(path string) string {
	return "'" + strings.ReplaceAll(layout.ToSlash(path), "'", "''") + "'"
}
