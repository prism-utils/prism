package logmeta

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/elk-utilities/prism/internal/store/layout"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

const labelIndexName = ".label_index.json"

// IndexedLabels are hot label columns served from the cardinality index.
var IndexedLabels = []string{"format", "template", "stream", "job"}

// LabelIndex stores distinct label values per tenant.
type LabelIndex struct {
	Generation uint64              `json:"generation"`
	Values     map[string][]string `json:"values"`
}

func labelIndexPath(dataDir, tenant string) string {
	return filepath.Join(dataDir, tenant, "logs", labelIndexName)
}

// IsIndexedLabel reports whether name is served from the cardinality index.
func IsIndexedLabel(name string) bool {
	for _, l := range IndexedLabels {
		if l == name {
			return true
		}
	}
	return false
}

// ReadLabelIndex loads the index; missing file yields empty index.
func ReadLabelIndex(dataDir, tenant string) (LabelIndex, error) {
	path := labelIndexPath(dataDir, tenant)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return LabelIndex{Values: map[string][]string{}}, nil
		}
		return LabelIndex{}, err
	}
	var idx LabelIndex
	if err := json.Unmarshal(b, &idx); err != nil {
		return LabelIndex{}, fmt.Errorf("logmeta: parse label index: %w", err)
	}
	if idx.Values == nil {
		idx.Values = map[string][]string{}
	}
	return idx, nil
}

// WriteLabelIndex atomically persists the index.
func WriteLabelIndex(dataDir, tenant string, idx LabelIndex) error {
	path := labelIndexPath(dataDir, tenant)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	for k, vals := range idx.Values {
		sort.Strings(vals)
		idx.Values[k] = vals
	}
	b, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// MergeLabelIndexFromParquet adds distinct indexed-column values from one file.
func MergeLabelIndexFromParquet(dataDir, tenant, parquetPath string) error {
	gen, err := Read(dataDir, tenant)
	if err != nil {
		return err
	}
	idx, err := ReadLabelIndex(dataDir, tenant)
	if err != nil {
		return err
	}
	if idx.Values == nil {
		idx.Values = map[string][]string{}
	}
	deltas, indexErr := distinctIndexedValues(parquetPath)
	if indexErr != nil {
		// Non-parquet or schema-less payloads skip index enrichment; land still succeeds.
		return nil //nolint:nilerr // index enrichment is best-effort on land
	}
	for label, vals := range deltas {
		set := map[string]struct{}{}
		for _, v := range idx.Values[label] {
			set[v] = struct{}{}
		}
		for _, v := range vals {
			set[v] = struct{}{}
		}
		out := make([]string, 0, len(set))
		for v := range set {
			out = append(out, v)
		}
		sort.Strings(out)
		idx.Values[label] = out
	}
	idx.Generation = gen
	return WriteLabelIndex(dataDir, tenant, idx)
}

// EnsureLabelIndex rebuilds the index when the generation stamp is stale.
func EnsureLabelIndex(dataDir, tenant string) (LabelIndex, error) {
	gen, err := Read(dataDir, tenant)
	if err != nil {
		return LabelIndex{}, err
	}
	idx, err := ReadLabelIndex(dataDir, tenant)
	if err != nil {
		return LabelIndex{}, err
	}
	if idx.Generation == gen && len(idx.Values) > 0 {
		return idx, nil
	}
	idx = LabelIndex{Generation: gen, Values: map[string][]string{}}
	artifacts, err := listLogArtifacts(dataDir, tenant)
	if err != nil {
		return LabelIndex{}, err
	}
	for _, artifact := range artifacts {
		m, err := ReadManifest(dataDir, tenant, artifact)
		if err != nil {
			return LabelIndex{}, err
		}
		if _, statErr := os.Stat(ManifestPath(dataDir, tenant, artifact)); statErr != nil || m.Version != gen || len(m.Files) == 0 {
			m, err = RebuildManifest(dataDir, tenant, artifact, gen)
			if err != nil {
				return LabelIndex{}, err
			}
		}
		artifactRoot := layout.LogsLandingDir(dataDir, tenant, artifact)
		for _, f := range m.Files {
			abs := filepath.Join(artifactRoot, filepath.FromSlash(f.Path))
			deltas, err := distinctIndexedValues(abs)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return LabelIndex{}, err
			}
			for label, vals := range deltas {
				set := map[string]struct{}{}
				for _, v := range idx.Values[label] {
					set[v] = struct{}{}
				}
				for _, v := range vals {
					set[v] = struct{}{}
				}
				out := make([]string, 0, len(set))
				for v := range set {
					out = append(out, v)
				}
				sort.Strings(out)
				idx.Values[label] = out
			}
		}
	}
	if err := WriteLabelIndex(dataDir, tenant, idx); err != nil {
		return LabelIndex{}, err
	}
	return idx, nil
}

// LabelValues returns distinct values for an indexed label (limit 0 = all).
func LabelValues(dataDir, tenant, label string, limit int) ([]string, error) {
	if !IsIndexedLabel(label) {
		return nil, fmt.Errorf("logmeta: label %q is not indexed", label)
	}
	idx, err := EnsureLabelIndex(dataDir, tenant)
	if err != nil {
		return nil, err
	}
	vals := idx.Values[label]
	if limit > 0 && len(vals) > limit {
		vals = vals[:limit]
	}
	out := append([]string(nil), vals...)
	return out, nil
}

func distinctIndexedValues(parquetPath string) (map[string][]string, error) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	cols, err := parquetColumns(db, parquetPath)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, label := range IndexedLabels {
		if !cols[label] {
			continue
		}
		//nolint:gosec // G201: label is a fixed allowlist; path is server-owned.
		q := fmt.Sprintf(
			`SELECT DISTINCT NULLIF(CAST(%s AS VARCHAR), '') AS v FROM read_parquet('%s') WHERE v IS NOT NULL ORDER BY v`,
			quoteIdent(label), layout.ToSlash(parquetPath),
		)
		rows, err := db.QueryContext(context.Background(), q) //nolint:contextcheck // sync index build has no request ctx
		if err != nil {
			return nil, err
		}
		var vals []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				_ = rows.Close()
				return nil, err
			}
			vals = append(vals, v)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(vals) > 0 {
			out[label] = vals
		}
	}
	return out, nil
}

func parquetColumns(db *sql.DB, path string) (map[string]bool, error) {
	//nolint:gosec // G201: path is server-owned under the tenant data dir.
	rows, err := db.QueryContext(context.Background(), //nolint:contextcheck // sync land/index helpers have no request ctx
		fmt.Sprintf("SELECT * FROM read_parquet('%s') LIMIT 0", layout.ToSlash(path)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, n := range names {
		out[n] = true
	}
	return out, nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func listLogArtifacts(dataDir, tenant string) ([]string, error) {
	logsRoot := filepath.Join(dataDir, tenant, "logs")
	entries, err := os.ReadDir(logsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "logs-") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}
