// Package materialize runs named read-only SQL at merge time and writes
// per-name parquet under <tenant>/materializations/<name>/.
package materialize

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Plane selects which merge kind an item attaches to.
type Plane string

const (
	// PlaneMetrics runs on metrics-tier merges (default).
	PlaneMetrics Plane = "metrics"
	// PlaneLogs runs on logs landing/tier merges.
	PlaneLogs Plane = "logs"
)

var nameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// Item is one named materialization query.
type Item struct {
	Name    string `json:"name" yaml:"name"`
	SQL     string `json:"sql" yaml:"sql"`
	On      string `json:"on" yaml:"on"`
	Format  string `json:"format" yaml:"format"`
	MinTier int    `json:"minTier" yaml:"minTier"`
}

// File is the on-disk YAML (or JSON) document pointed at by MATERIALIZATIONS_FILE.
type File struct {
	Materializations []Item `json:"materializations" yaml:"materializations"`
}

// Load reads a materialization file. An empty path is a no-op (no items).
func Load(path string) (File, error) {
	if strings.TrimSpace(path) == "" {
		return File{}, nil
	}
	body, err := os.ReadFile(path) //nolint:gosec // G304: operator-supplied config path
	if err != nil {
		return File{}, fmt.Errorf("materializations: read %s: %w", path, err)
	}
	var f File
	if err := yaml.Unmarshal(body, &f); err != nil {
		return File{}, fmt.Errorf("materializations: parse %s: %w", path, err)
	}
	return f, nil
}

// Names returns configured materialization identifiers in file order.
func (f File) Names() []string {
	out := make([]string, 0, len(f.Materializations))
	for _, it := range f.Materializations {
		out = append(out, it.Name)
	}
	return out
}

// Validate rejects invalid names, non-SELECT SQL, and unknown planes/formats.
func (f File) Validate() error {
	seen := make(map[string]struct{}, len(f.Materializations))
	for i, it := range f.Materializations {
		if err := it.validate(i); err != nil {
			return err
		}
		if _, dup := seen[it.Name]; dup {
			return fmt.Errorf("materializations[%d].name: duplicate %q", i, it.Name)
		}
		seen[it.Name] = struct{}{}
	}
	return nil
}

func (it Item) validate(i int) error {
	if !nameRe.MatchString(it.Name) {
		return fmt.Errorf("materializations[%d].name: %q must match %s", i, it.Name, nameRe.String())
	}
	if strings.Contains(it.Name, "..") || strings.ContainsAny(it.Name, `/\`) {
		return fmt.Errorf("materializations[%d].name: path traversal is not allowed", i)
	}
	if err := validateReadOnlySQL(it.SQL); err != nil {
		return fmt.Errorf("materializations[%d].sql: %w", i, err)
	}
	on := strings.ToLower(strings.TrimSpace(it.On))
	if on != "" && on != string(PlaneMetrics) && on != string(PlaneLogs) {
		return fmt.Errorf("materializations[%d].on: %q must be metrics or logs", i, it.On)
	}
	format := strings.ToLower(strings.TrimSpace(it.Format))
	if format != "" && format != "parquet" && format != "duckdb" {
		return fmt.Errorf("materializations[%d].format: %q must be parquet or duckdb", i, it.Format)
	}
	if it.MinTier < 0 {
		return fmt.Errorf("materializations[%d].minTier: must be >= 0", i)
	}
	return nil
}

func (it Item) plane() Plane {
	switch strings.ToLower(strings.TrimSpace(it.On)) {
	case string(PlaneLogs):
		return PlaneLogs
	default:
		return PlaneMetrics
	}
}
