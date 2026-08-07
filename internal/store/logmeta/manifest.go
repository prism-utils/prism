package logmeta

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
)

const manifestName = "_manifest.json"

// ManifestFile describes one on-disk log parquet relative to its artifact dir.
type ManifestFile struct {
	Path    string `json:"path"`
	MinTsNs int64  `json:"min_ts_ns"`
	MaxTsNs int64  `json:"max_ts_ns"`
	Bytes   int64  `json:"bytes"`
}

// Manifest is the artifact-level open-set catalog written on land/merge/retention.
type Manifest struct {
	Version uint64         `json:"version"`
	Files   []ManifestFile `json:"files"`
}

// ManifestPath returns the manifest file path for one logs artifact.
func ManifestPath(dataDir, tenant, artifact string) string {
	return filepath.Join(dataDir, tenant, "logs", artifact, manifestName)
}

// WriteManifest atomically writes a manifest for one artifact.
func WriteManifest(dataDir, tenant, artifact string, m Manifest) error {
	path := ManifestPath(dataDir, tenant, artifact)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadManifest loads a manifest; missing file yields empty manifest, nil error.
func ReadManifest(dataDir, tenant, artifact string) (Manifest, error) {
	path := ManifestPath(dataDir, tenant, artifact)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, nil
		}
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return Manifest{}, fmt.Errorf("logmeta: parse manifest: %w", err)
	}
	return m, nil
}

// isSegmentName reports whether a filename is a log segment the query planners
// can open. Tiers hold MERGE_SEGMENT_FORMAT output, so both extensions belong in
// the catalog: omitting duckdb segments hides every refreshed row.
func isSegmentName(name string) bool {
	switch filepath.Ext(name) {
	case ".parquet", ".duckdb":
		return true
	default:
		return false
	}
}

// RebuildManifest scans landing + tiers for one artifact and returns a manifest
// tagged with version.
func RebuildManifest(dataDir, tenant, artifact string, version uint64) (Manifest, error) {
	artifactRoot := layout.LogsLandingDir(dataDir, tenant, artifact)
	var files []ManifestFile
	walk := func(root, relPrefix string) error {
		entries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		for _, e := range entries {
			if e.IsDir() || !isSegmentName(e.Name()) || e.Name()[0] == '.' {
				continue
			}
			abs := filepath.Join(root, e.Name())
			fi, err := os.Stat(abs)
			if err != nil {
				return err
			}
			rel := e.Name()
			if relPrefix != "" {
				rel = filepath.ToSlash(filepath.Join(relPrefix, e.Name()))
			}
			minNs, maxNs := fileTimeBounds(abs, fi.ModTime())
			files = append(files, ManifestFile{
				Path:    rel,
				MinTsNs: minNs,
				MaxTsNs: maxNs,
				Bytes:   fi.Size(),
			})
		}
		return nil
	}
	if err := walk(artifactRoot, ""); err != nil {
		return Manifest{}, err
	}
	tiersRoot := filepath.Join(artifactRoot, "tiers")
	tierEntries, err := os.ReadDir(tiersRoot)
	if err != nil && !os.IsNotExist(err) {
		return Manifest{}, err
	}
	for _, te := range tierEntries {
		if !te.IsDir() || !strings.HasPrefix(te.Name(), "L") {
			continue
		}
		if err := walk(filepath.Join(tiersRoot, te.Name()), filepath.Join("tiers", te.Name())); err != nil {
			return Manifest{}, err
		}
	}
	return Manifest{Version: version, Files: files}, nil
}

// SyncManifest rebuilds and writes the manifest for one artifact at the current generation.
func SyncManifest(dataDir, tenant, artifact string) error {
	gen, err := Read(dataDir, tenant)
	if err != nil {
		return err
	}
	m, err := RebuildManifest(dataDir, tenant, artifact, gen)
	if err != nil {
		return err
	}
	return WriteManifest(dataDir, tenant, artifact, m)
}

func fileTimeBounds(path string, mtime time.Time) (minNs, maxNs int64) {
	if ns, ok := layout.WindowIDNanos(path); ok {
		return ns, ns
	}
	ns := mtime.UTC().UnixNano()
	return ns, ns
}
