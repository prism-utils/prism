package logmeta

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/metrics"
)

// Catalog is the in-memory log open-set for one artifact, keyed by relative path.
type Catalog struct {
	mu       sync.Mutex
	dataDir  string
	coldDir  string
	tenant   string
	artifact string
	byPath   map[string]ManifestFile
	rebuilt  bool
}

// Load hydrates from JSON. A missing file is empty. Parse failure rebuilds.
func Load(_ context.Context, dataDir, coldDir, tenant, artifact string) (*Catalog, error) {
	c := &Catalog{
		dataDir:  dataDir,
		coldDir:  coldDir,
		tenant:   tenant,
		artifact: artifact,
		byPath:   map[string]ManifestFile{},
	}
	path := ManifestPath(dataDir, tenant, artifact)
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is dataDir/tenant/logs/<artifact>/_manifest.json
	if err != nil {
		if os.IsNotExist(err) {
			return c, nil
		}
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		slog.Error("logs catalog full rebuild",
			"tenant", tenant,
			"reason", "corrupt",
			"path", path,
			"err", err,
		)
		metrics.CatalogRebuild(metrics.PlaneLogs, metrics.RebuildCorrupt)
		gen, gerr := Read(dataDir, tenant)
		if gerr != nil {
			return nil, gerr
		}
		rebuilt, rerr := RebuildManifestRoots(dataDir, coldDir, tenant, artifact, gen)
		if rerr != nil {
			return nil, rerr
		}
		if werr := WriteManifest(dataDir, tenant, artifact, rebuilt); werr != nil {
			return nil, werr
		}
		c.rebuilt = true
		c.replace(rebuilt.Files)
		return c, nil
	}
	c.replace(m.Files)
	return c, nil
}

// Rebuilt reports whether this handle was filled by a corrupt-JSON rebuild.
func (c *Catalog) Rebuilt() bool {
	if c == nil {
		return false
	}
	return c.rebuilt
}

// Lookup hits only when path, size, and mtime all match.
func (c *Catalog) Lookup(rel string, size, mtimeNs int64) (ManifestFile, bool) {
	if c == nil {
		return ManifestFile{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	f, ok := c.byPath[rel]
	if !ok || f.Bytes != size || f.MtimeNs != mtimeNs {
		return ManifestFile{}, false
	}
	return f, true
}

// Upsert stores one file's bounds, size, and mtime.
func (c *Catalog) Upsert(f ManifestFile) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byPath == nil {
		c.byPath = map[string]ManifestFile{}
	}
	f.Path = filepath.ToSlash(f.Path)
	c.byPath[f.Path] = f
}

// Drop removes relative paths from the catalog.
func (c *Catalog) Drop(rels ...string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rel := range rels {
		delete(c.byPath, filepath.ToSlash(rel))
	}
}

// Files returns a copy of catalog entries.
func (c *Catalog) Files() []ManifestFile {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ManifestFile, 0, len(c.byPath))
	for _, f := range c.byPath {
		out = append(out, f)
	}
	return out
}

// DropMissing removes tier entries that were not seen in this scan.
func (c *Catalog) DropMissing(live []string) {
	if c == nil {
		return
	}
	keep := make(map[string]struct{}, len(live))
	for _, rel := range live {
		keep[filepath.ToSlash(rel)] = struct{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for rel := range c.byPath {
		if !strings.HasPrefix(rel, "tiers/") {
			continue
		}
		if _, ok := keep[rel]; !ok {
			delete(c.byPath, rel)
		}
	}
}

// Persist writes the catalog atomically.
func (c *Catalog) Persist() error {
	if c == nil {
		return nil
	}
	gen, err := Read(c.dataDir, c.tenant)
	if err != nil {
		return err
	}
	return WriteManifest(c.dataDir, c.tenant, c.artifact, Manifest{Version: gen, Files: c.Files()})
}

// ApplyDelta upserts dest files, drops sources, and persists without a full rebuild.
func ApplyDelta(dataDir, coldDir, tenant, artifact string, upsert []ManifestFile, drop []string) error {
	cat, err := Load(context.Background(), dataDir, coldDir, tenant, artifact)
	if err != nil {
		return err
	}
	for _, f := range upsert {
		cat.Upsert(f)
	}
	cat.Drop(drop...)
	if len(upsert) == 0 && len(drop) == 0 {
		if err := cat.fillMisses(); err != nil {
			return err
		}
	}
	return cat.Persist()
}

func (c *Catalog) replace(files []ManifestFile) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byPath = make(map[string]ManifestFile, len(files))
	for _, f := range files {
		f.Path = filepath.ToSlash(f.Path)
		c.byPath[f.Path] = f
	}
}

func (c *Catalog) fillMisses() error {
	live := map[string]struct{}{}
	walk := func(root, relPrefix string) error {
		entries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		retired := layout.CompactedSet(entries)
		for _, e := range entries {
			if e.IsDir() || !isSegmentName(e.Name()) || e.Name()[0] == '.' {
				continue
			}
			if _, held := retired[e.Name()]; held {
				continue
			}
			rel := e.Name()
			if relPrefix != "" {
				rel = filepath.ToSlash(filepath.Join(relPrefix, e.Name()))
			}
			abs := filepath.Join(root, e.Name())
			fi, err := os.Stat(abs)
			if err != nil {
				return err
			}
			live[rel] = struct{}{}
			if _, hit := c.Lookup(rel, fi.Size(), fi.ModTime().UnixNano()); hit {
				continue
			}
			minNs, maxNs := fileTimeBounds(abs, fi.ModTime())
			c.Upsert(ManifestFile{
				Path:    rel,
				MinTsNs: minNs,
				MaxTsNs: maxNs,
				Bytes:   fi.Size(),
				MtimeNs: fi.ModTime().UnixNano(),
			})
		}
		return nil
	}
	artifactRoot := layout.LogsLandingDir(c.dataDir, c.tenant, c.artifact)
	if err := walk(artifactRoot, ""); err != nil {
		return err
	}
	walkTiers := func(root string) error {
		tiersRoot := filepath.Join(layout.LogsLandingDir(root, c.tenant, c.artifact), "tiers")
		tierEntries, err := os.ReadDir(tiersRoot)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		for _, te := range tierEntries {
			if !te.IsDir() || !strings.HasPrefix(te.Name(), "L") {
				continue
			}
			if err := walk(filepath.Join(tiersRoot, te.Name()), filepath.Join("tiers", te.Name())); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walkTiers(c.dataDir); err != nil {
		return err
	}
	if layout.ColdEnabled(c.coldDir) {
		if err := walkTiers(c.coldDir); err != nil {
			return err
		}
	}
	keep := make([]string, 0, len(live))
	for rel := range live {
		keep = append(keep, rel)
	}
	c.dropNotLive(keep)
	return nil
}

func (c *Catalog) dropNotLive(live []string) {
	keep := make(map[string]struct{}, len(live))
	for _, rel := range live {
		keep[filepath.ToSlash(rel)] = struct{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for rel := range c.byPath {
		if _, ok := keep[rel]; !ok {
			delete(c.byPath, rel)
		}
	}
}
