package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
)

// Options selects a tenant store root to snapshot.
type Options struct {
	DataDir string
	ColdDir string
	Tenant  string
	Now     time.Time
}

// Report is the JSON histogram of one tenant's on-disk segments.
type Report struct {
	Tenant        string           `json:"tenant"`
	DataDir       string           `json:"data_dir"`
	ColdDir       string           `json:"cold_dir,omitempty"`
	GeneratedAt   time.Time        `json:"generated_at"`
	Totals        Count            `json:"totals"`
	ByFamily      map[string]Count `json:"by_family"`
	ByKind        map[string]Count `json:"by_kind"`
	ByTier        map[string]Count `json:"by_tier"`
	ByRoot        map[string]Count `json:"by_root"`
	SizeHistogram []SizeBucket     `json:"size_histogram"`
	DateHistogram []DayBucket      `json:"date_histogram"`
	Segments      []File           `json:"segments,omitempty"`
}

// Count is files + bytes (and unreadable, on totals only).
type Count struct {
	Files      int   `json:"files"`
	Bytes      int64 `json:"bytes"`
	Unreadable int   `json:"unreadable,omitempty"`
}

// SizeBucket is one non-overlapping size bin. LeBytes is the inclusive upper
// bound; nil means +Inf.
type SizeBucket struct {
	LeBytes *int64 `json:"le_bytes"`
	Count
}

// DayBucket counts files whose min_ts falls on that UTC calendar day.
type DayBucket struct {
	Day string `json:"day"`
	Count
}

// File is one live segment (parquet or duckdb).
type File struct {
	Rel       string    `json:"path"`
	Root      string    `json:"root"`
	Family    string    `json:"family"`
	Kind      string    `json:"kind"`
	Artifact  string    `json:"artifact,omitempty"`
	Step      string    `json:"step,omitempty"`
	MatName   string    `json:"materialization,omitempty"`
	Tier      int       `json:"tier"`
	Bytes     int64     `json:"bytes"`
	Rows      int64     `json:"rows,omitempty"`
	MinTS     time.Time `json:"min_ts"`
	MaxTS     time.Time `json:"max_ts"`
	MTime     time.Time `json:"mtime"`
	BoundsErr string    `json:"bounds_err,omitempty"`
}

var sizeBounds = []int64{
	4 << 10,
	16 << 10,
	64 << 10,
	256 << 10,
	1 << 20,
	4 << 20,
	16 << 20,
	64 << 20,
	256 << 20,
	1 << 30,
	4 << 30,
}

// Snapshot walks a tenant store and builds the histogram report.
func Snapshot(opts Options) (Report, error) {
	if err := validateTenant(opts.Tenant); err != nil {
		return Report{}, err
	}
	if strings.TrimSpace(opts.DataDir) == "" {
		return Report{}, fmt.Errorf("data-dir is required")
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	hotRoot := layout.TenantDir(opts.DataDir, opts.Tenant)
	if err := mustTenantDir(hotRoot); err != nil {
		return Report{}, err
	}
	rep := Report{
		Tenant:      opts.Tenant,
		DataDir:     opts.DataDir,
		ColdDir:     strings.TrimSpace(opts.ColdDir),
		GeneratedAt: now,
		ByFamily:    map[string]Count{},
		ByKind:      map[string]Count{},
		ByTier:      map[string]Count{},
		ByRoot:      map[string]Count{},
	}
	if err := walkRoot(&rep, hotRoot, "hot"); err != nil {
		return Report{}, err
	}
	if layout.ColdEnabled(opts.ColdDir) {
		coldRoot := layout.TenantDir(opts.ColdDir, opts.Tenant)
		if st, err := os.Stat(coldRoot); err == nil && st.IsDir() {
			if err := walkRoot(&rep, coldRoot, "cold"); err != nil {
				return Report{}, err
			}
		}
	}
	sort.Slice(rep.Segments, func(i, j int) bool {
		if rep.Segments[i].Root != rep.Segments[j].Root {
			return rep.Segments[i].Root < rep.Segments[j].Root
		}
		return rep.Segments[i].Rel < rep.Segments[j].Rel
	})
	rep.SizeHistogram = sizeHistogram(rep.Segments)
	rep.DateHistogram = dateHistogram(rep.Segments)
	return rep, nil
}

func validateTenant(tenant string) error {
	if tenant == "" || tenant == "." || tenant == ".." {
		return fmt.Errorf("tenant is required")
	}
	if filepath.IsAbs(tenant) {
		return fmt.Errorf("tenant must be a single path element")
	}
	if strings.Contains(tenant, "..") || strings.ContainsAny(tenant, `/\`) {
		return fmt.Errorf("tenant must be a single path element")
	}
	if filepath.Base(tenant) != tenant {
		return fmt.Errorf("tenant must be a single path element")
	}
	return nil
}

func mustTenantDir(dir string) error {
	st, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("tenant dir: %w", err)
	}
	if !st.IsDir() {
		return fmt.Errorf("tenant dir is not a directory")
	}
	return nil
}

func walkRoot(rep *Report, root, rootName string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if layout.IsCompactedMarker(name) || layout.IsCompactedMarkerTemp(name) {
			return nil
		}
		if strings.HasPrefix(name, "_") || strings.HasPrefix(name, ".") {
			return nil
		}
		if strings.HasSuffix(name, ".tmp") || strings.HasSuffix(name, ".merge-skip") {
			return nil
		}
		if !isSegmentName(name) {
			return nil
		}
		if _, err := os.Stat(path + ".compacted"); err == nil {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		f, ok := classify(rel)
		if !ok {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		f.Root = rootName
		f.Rel = rel
		f.Bytes = info.Size()
		f.MTime = info.ModTime().UTC()
		minTS, maxTS, rows, berr := segmentBounds(path)
		f.Rows = rows
		f.MinTS = minTS
		f.MaxTS = maxTS
		if berr != nil {
			f.BoundsErr = berr.Error()
			rep.Totals.Unreadable++
		}
		addFile(rep, &f)
		return nil
	})
}

func isSegmentName(name string) bool {
	return strings.HasSuffix(name, ".parquet") || strings.HasSuffix(name, ".duckdb")
}

func classify(rel string) (File, bool) {
	parts := strings.Split(rel, "/")
	switch {
	case len(parts) == 2 && parts[0] == "hot" && (parts[1] == "current.parquet" || parts[1] == "current.duckdb"):
		return File{Family: "metrics", Kind: "hot", Tier: -1}, true
	case len(parts) == 3 && parts[0] == "tiers" && strings.HasPrefix(parts[1], "L"):
		tier, ok := parseTier(parts[1])
		if !ok {
			return File{}, false
		}
		return File{Family: "metrics", Kind: "tier", Tier: tier}, true
	case len(parts) == 5 && parts[0] == "logs" && parts[2] == "tiers" && strings.HasPrefix(parts[3], "L"):
		tier, ok := parseTier(parts[3])
		if !ok {
			return File{}, false
		}
		return File{Family: "logs", Kind: "tier", Artifact: parts[1], Tier: tier}, true
	case len(parts) == 3 && parts[0] == "logs":
		return File{Family: "logs", Kind: "landing", Artifact: parts[1], Tier: -1}, true
	case len(parts) == 3 && parts[0] == "rollups":
		return File{Family: "rollup", Kind: "rollup", Step: parts[1], Tier: -1}, true
	case len(parts) == 3 && parts[0] == "materializations":
		return File{Family: "materialization", Kind: "materialization", MatName: parts[1], Tier: -1}, true
	default:
		return File{Family: "other", Kind: "file", Tier: -1}, true
	}
}

func parseTier(name string) (int, bool) {
	if !strings.HasPrefix(name, "L") {
		return 0, false
	}
	n, err := strconv.Atoi(name[1:])
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func addFile(rep *Report, f *File) {
	rep.Segments = append(rep.Segments, *f)
	c := Count{Files: 1, Bytes: f.Bytes}
	rep.Totals.Files++
	rep.Totals.Bytes += f.Bytes
	rep.ByFamily[f.Family] = addCount(rep.ByFamily[f.Family], c)
	rep.ByKind[f.Kind] = addCount(rep.ByKind[f.Kind], c)
	rep.ByRoot[f.Root] = addCount(rep.ByRoot[f.Root], c)
	if f.Kind == "tier" && f.Tier >= 0 {
		key := fmt.Sprintf("L%d", f.Tier)
		rep.ByTier[key] = addCount(rep.ByTier[key], c)
	}
}

func addCount(a, b Count) Count {
	a.Files += b.Files
	a.Bytes += b.Bytes
	return a
}

func sizeHistogram(files []File) []SizeBucket {
	out := make([]SizeBucket, 0, len(sizeBounds)+1)
	counts := make([]SizeBucket, len(sizeBounds)+1)
	for i, le := range sizeBounds {
		v := le
		counts[i].LeBytes = &v
	}
	for i := range files {
		f := &files[i]
		idx := len(sizeBounds)
		for j, le := range sizeBounds {
			if f.Bytes <= le {
				idx = j
				break
			}
		}
		counts[idx].Files++
		counts[idx].Bytes += f.Bytes
	}
	for i := range counts {
		if counts[i].Files == 0 {
			continue
		}
		out = append(out, counts[i])
	}
	return out
}

func dateHistogram(files []File) []DayBucket {
	byDay := map[string]Count{}
	for i := range files {
		f := &files[i]
		if f.MinTS.IsZero() {
			byDay["unknown"] = addCount(byDay["unknown"], Count{Files: 1, Bytes: f.Bytes})
			continue
		}
		day := f.MinTS.UTC().Format("2006-01-02")
		byDay[day] = addCount(byDay[day], Count{Files: 1, Bytes: f.Bytes})
	}
	days := make([]string, 0, len(byDay))
	for d := range byDay {
		days = append(days, d)
	}
	sort.Strings(days)
	out := make([]DayBucket, 0, len(days))
	for _, d := range days {
		out = append(out, DayBucket{Day: d, Count: byDay[d]})
	}
	return out
}

func segmentBounds(path string) (minTS, maxTS time.Time, rows int64, err error) {
	if ns, ok := layout.WindowIDNanos(path); ok {
		minTS = time.Unix(0, ns).UTC()
		maxTS = minTS
	}
	if strings.HasSuffix(strings.ToLower(path), ".duckdb") {
		return minTS, maxTS, 0, nil
	}
	pmin, pmax, nrows, perr := parquetTimeBounds(path)
	if perr != nil {
		return minTS, maxTS, 0, perr
	}
	if !pmin.IsZero() {
		minTS, maxTS = pmin, pmax
	}
	return minTS, maxTS, nrows, nil
}

func encodeReport(w io.Writer, rep *Report, compact bool) error {
	enc := json.NewEncoder(w)
	if !compact {
		enc.SetIndent("", "  ")
	}
	return enc.Encode(rep)
}
