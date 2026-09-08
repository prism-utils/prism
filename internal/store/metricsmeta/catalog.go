package metricsmeta

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/metrics"
)

// Catalog is the in-memory metrics open-set keyed by tenant-relative path.
type Catalog struct {
	mu      sync.Mutex
	dataDir string
	coldDir string
	tenant  string
	byPath  map[string]ManifestFile
	rebuilt bool
}

// Load hydrates from JSON. A missing file is empty. Parse failure rebuilds.
func Load(ctx context.Context, dataDir, coldDir, tenant string) (*Catalog, error) {
	c := &Catalog{
		dataDir: dataDir,
		coldDir: coldDir,
		tenant:  tenant,
		byPath:  map[string]ManifestFile{},
	}
	path := ManifestPath(dataDir, tenant)
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is dataDir/tenant/tiers/_manifest.json
	if err != nil {
		if os.IsNotExist(err) {
			return c, nil
		}
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		slog.Error("metrics catalog full rebuild",
			"tenant", tenant,
			"reason", "corrupt",
			"path", path,
			"err", err,
		)
		metrics.CatalogRebuild(metrics.PlaneMetrics, metrics.RebuildCorrupt)
		gen, gerr := ReadGeneration(dataDir, tenant)
		if gerr != nil {
			return nil, gerr
		}
		rebuilt, rerr := RebuildManifestRoots(ctx, dataDir, coldDir, tenant, gen)
		if rerr != nil {
			return nil, rerr
		}
		if werr := WriteManifest(dataDir, tenant, rebuilt); werr != nil {
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
	gen, err := ReadGeneration(c.dataDir, c.tenant)
	if err != nil {
		return err
	}
	return WriteManifest(c.dataDir, c.tenant, Manifest{Version: gen, Files: c.Files()})
}

// ApplyDelta bumps generation, upserts dest files, drops sources, and persists.
func ApplyDelta(ctx context.Context, dataDir, coldDir, tenant string, upsert []ManifestFile, drop []string) error {
	if err := Bump(dataDir, tenant); err != nil {
		return err
	}
	cat, err := Load(ctx, dataDir, coldDir, tenant)
	if err != nil {
		return err
	}
	for _, f := range upsert {
		cat.Upsert(f)
	}
	cat.Drop(drop...)
	if len(upsert) == 0 && len(drop) == 0 {
		if err := cat.fillMisses(ctx); err != nil {
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

func (c *Catalog) fillMisses(ctx context.Context) error {
	live := map[string]struct{}{}
	add := func(abs, rel string) error {
		rel = filepath.ToSlash(rel)
		fi, err := os.Stat(abs)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		live[rel] = struct{}{}
		if _, hit := c.Lookup(rel, fi.Size(), fi.ModTime().UnixNano()); hit {
			return nil
		}
		minNs, maxNs, ok := FileBounds(ctx, abs)
		if !ok {
			return nil
		}
		c.Upsert(ManifestFile{
			Path:    rel,
			MinTsNs: minNs,
			MaxTsNs: maxNs,
			Bytes:   fi.Size(),
			MtimeNs: fi.ModTime().UnixNano(),
		})
		return nil
	}
	tenantRoot := filepath.Join(c.dataDir, c.tenant)
	for _, rel := range []string{hotRelParquet, hotRelDuckDB} {
		if err := add(filepath.Join(tenantRoot, filepath.FromSlash(rel)), rel); err != nil {
			return err
		}
	}
	scan := func(root string, minTier int) error {
		for tier := minTier; tier <= 8; tier++ {
			dir := layout.TierDir(root, c.tenant, tier)
			entries, err := os.ReadDir(dir)
			if err != nil {
				if os.IsNotExist(err) {
					continue
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
				rel := filepath.ToSlash(filepath.Join("tiers", fmt.Sprintf("L%d", tier), e.Name()))
				if err := add(filepath.Join(dir, e.Name()), rel); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := scan(c.dataDir, 0); err != nil {
		return err
	}
	if layout.ColdEnabled(c.coldDir) {
		if err := scan(c.coldDir, 0); err != nil {
			return err
		}
	}
	var liveRels []string
	for rel := range live {
		liveRels = append(liveRels, rel)
	}
	c.DropMissing(liveRels)
	return nil
}
