package merge

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/logmeta"
	"github.com/prism-utils/prism/internal/store/metrics"
	"github.com/prism-utils/prism/internal/store/metricsmeta"
	"github.com/prism-utils/prism/internal/store/segformat"
)

var statSegment = StatSegment
var statLogSegment = StatLogSegment

func tierRel(tier int, name string) string {
	return filepath.ToSlash(filepath.Join("tiers", fmt.Sprintf("L%d", tier), name))
}

func scanTierDir(dir string, tier int, caps DuckDBCaps, cat *metricsmeta.Catalog) ([]Segment, []string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	retired := layout.CompactedSet(entries)
	skipped := layout.MergeSkipSet(entries)
	var out []Segment
	var live []string
	for _, e := range entries {
		if e.IsDir() || !isSegmentFile(e.Name()) {
			continue
		}
		if _, held := retired[e.Name()]; held {
			continue
		}
		if _, skip := skipped[e.Name()]; skip {
			continue
		}
		path := filepath.Join(dir, e.Name())
		fi, err := e.Info()
		if err != nil {
			return nil, nil, err
		}
		rel := tierRel(tier, e.Name())
		live = append(live, rel)
		if f, hit := cat.Lookup(rel, fi.Size(), fi.ModTime().UnixNano()); hit {
			metrics.CatalogLookup(metrics.PlaneMetrics, metrics.CatalogHit)
			out = append(out, Segment{
				Tier:  tier,
				Path:  path,
				Bytes: f.Bytes,
				MinTs: time.Unix(0, f.MinTsNs).UTC(),
				MaxTs: time.Unix(0, f.MaxTsNs).UTC(),
			})
			continue
		}
		metrics.CatalogLookup(metrics.PlaneMetrics, metrics.CatalogMiss)
		seg, err := statSegment(path, tier, caps)
		if err != nil {
			return nil, nil, err
		}
		cat.Upsert(metricsmeta.ManifestFile{
			Path:    rel,
			MinTsNs: seg.MinTs.UnixNano(),
			MaxTsNs: seg.MaxTs.UnixNano(),
			Bytes:   seg.Bytes,
			MtimeNs: fi.ModTime().UnixNano(),
		})
		out = append(out, seg)
	}
	return out, live, nil
}

func scanLogTierDir(dir string, tier int, cat *logmeta.Catalog) ([]Segment, []string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	retired := layout.CompactedSet(entries)
	skipped := layout.MergeSkipSet(entries)
	var out []Segment
	var live []string
	for _, e := range entries {
		if e.IsDir() || !isSegmentFile(e.Name()) {
			continue
		}
		if _, held := retired[e.Name()]; held {
			continue
		}
		if _, skip := skipped[e.Name()]; skip {
			continue
		}
		path := filepath.Join(dir, e.Name())
		fi, err := e.Info()
		if err != nil {
			return nil, nil, err
		}
		rel := tierRel(tier, e.Name())
		live = append(live, rel)
		if f, hit := cat.Lookup(rel, fi.Size(), fi.ModTime().UnixNano()); hit {
			metrics.CatalogLookup(metrics.PlaneLogs, metrics.CatalogHit)
			seg := Segment{
				Tier:  tier,
				Path:  path,
				Bytes: f.Bytes,
				MinTs: time.Unix(0, f.MinTsNs).UTC(),
				MaxTs: time.Unix(0, f.MaxTsNs).UTC(),
			}
			if segformat.SkipOpen(path, seg.Bytes) {
				continue
			}
			out = append(out, seg)
			continue
		}
		metrics.CatalogLookup(metrics.PlaneLogs, metrics.CatalogMiss)
		seg, err := statLogSegment(path, tier)
		if err != nil {
			return nil, nil, err
		}
		if segformat.SkipOpen(path, seg.Bytes) {
			continue
		}
		cat.Upsert(logmeta.ManifestFile{
			Path:    rel,
			MinTsNs: seg.MinTs.UnixNano(),
			MaxTsNs: seg.MaxTs.UnixNano(),
			Bytes:   seg.Bytes,
			MtimeNs: fi.ModTime().UnixNano(),
		})
		out = append(out, seg)
	}
	return out, live, nil
}

func persistMetricsScan(dataDir, coldDir, tenant string, maxTier int, caps DuckDBCaps) ([]Segment, error) {
	cat, err := metricsmeta.Load(context.Background(), dataDir, coldDir, tenant)
	if err != nil {
		return nil, err
	}
	var all []Segment
	var live []string
	scanRoot := func(root string) error {
		for tier := 0; tier <= maxTier; tier++ {
			segs, rels, err := scanTierDir(layout.TierDir(root, tenant, tier), tier, caps, cat)
			if err != nil {
				return err
			}
			all = append(all, segs...)
			live = append(live, rels...)
		}
		return nil
	}
	if err := scanRoot(dataDir); err != nil {
		return nil, err
	}
	if coldDir != "" {
		if err := scanRoot(coldDir); err != nil {
			return nil, err
		}
	}
	cat.DropMissing(live)
	if err := cat.Persist(); err != nil {
		return nil, err
	}
	return all, nil
}

func persistLogScan(dataDir, coldDir, tenant, artifact string, maxTier int) ([]Segment, error) {
	cat, err := logmeta.Load(context.Background(), dataDir, coldDir, tenant, artifact)
	if err != nil {
		return nil, err
	}
	var all []Segment
	var live []string
	scanRoot := func(root string) error {
		for tier := 0; tier <= maxTier; tier++ {
			segs, rels, err := scanLogTierDir(layout.LogsTierDir(root, tenant, artifact, tier), tier, cat)
			if err != nil {
				return err
			}
			all = append(all, segs...)
			live = append(live, rels...)
		}
		return nil
	}
	if err := scanRoot(dataDir); err != nil {
		return nil, err
	}
	if coldDir != "" {
		if err := scanRoot(coldDir); err != nil {
			return nil, err
		}
	}
	cat.DropMissing(live)
	if err := cat.Persist(); err != nil {
		return nil, err
	}
	return all, nil
}
