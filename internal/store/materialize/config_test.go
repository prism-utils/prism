package materialize

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadEmptyPathIsNoop(t *testing.T) {
	t.Parallel()
	f, err := Load("")
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if len(f.Materializations) != 0 {
		t.Fatalf("want no items, got %d", len(f.Materializations))
	}
}

func TestLoadMissingFileErrors(t *testing.T) {
	t.Parallel()
	_, err := Load(filepath.Join(t.TempDir(), "no-such.yaml"))
	if err == nil {
		t.Fatal("Load missing file: want error")
	}
}

func TestLoadValidYAML(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "mat.yaml")
	body := []byte(`
materializations:
  - name: last_events
    sql: SELECT 1 AS x FROM merge_output
    on: metrics
    minTier: 0
`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := f.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(f.Materializations) != 1 || f.Materializations[0].Name != "last_events" {
		t.Fatalf("items = %+v", f.Materializations)
	}
}

func TestValidateRejectsInvalidName(t *testing.T) {
	t.Parallel()
	cases := []Item{
		{Name: "../etc", SQL: "SELECT 1"},
		{Name: "HasCaps", SQL: "SELECT 1"},
		{Name: "bad-dash", SQL: "SELECT 1"},
		{Name: "", SQL: "SELECT 1"},
		{Name: "ok", SQL: "COPY (SELECT 1) TO 'x'"},
		{Name: "ok", SQL: "INSERT INTO t VALUES (1)"},
		{Name: "ok", SQL: "SELECT 1; SELECT 2"},
		{Name: "ok", SQL: ""},
	}
	for _, item := range cases {
		f := File{Materializations: []Item{item}}
		if err := f.Validate(); err == nil {
			t.Fatalf("Validate(%+v) = nil, want error", item)
		}
	}
}

func TestValidateAcceptsSelectAndWith(t *testing.T) {
	t.Parallel()
	f := File{Materializations: []Item{
		{Name: "plain", SQL: "SELECT COUNT(*) AS n FROM merge_output"},
		{Name: "cte", SQL: "WITH x AS (SELECT 1 AS a) SELECT a FROM x"},
	}}
	if err := f.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateNamesOffendingPath(t *testing.T) {
	t.Parallel()
	f := File{Materializations: []Item{{Name: "bad name", SQL: "SELECT 1"}}}
	err := f.Validate()
	if err == nil {
		t.Fatal("want error")
	}
	if !strings.Contains(err.Error(), "materializations[0].name") {
		t.Fatalf("error %q should name config path", err)
	}
}
