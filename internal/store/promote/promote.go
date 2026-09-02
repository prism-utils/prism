package promote

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/seed"
	"github.com/prism-utils/prism/internal/store/segformat"
)

const defaultAfter = 12 * time.Hour

// Config drives one tenant promote pass. An empty ColdDir disables all work.
type Config struct {
	DataDir string
	ColdDir string
	After   time.Duration
	MaxTier int
	Grace   time.Duration
	Now     func() time.Time
	// MaxTs returns the data-age clock for a live segment. Tests inject a stub;
	// production supplies catalog or footer bounds.
	MaxTs func(path string) (time.Time, bool)
	// AfterPromote runs once a dest is durable (catalog rebuild, generation bump).
	AfterPromote func(tenant string) error
	// HoldSource marks the hot source for delete grace instead of unlinking it
	// immediately. Nil unlinks as soon as dest verifies.
	HoldSource func(path string, until time.Time) error
}

// Enabled reports whether promote should run.
func Enabled(coldDir string) bool {
	return layout.ColdEnabled(coldDir)
}

// Eligible reports whether a compacted segment may leave the hot root.
// Any tier whose data is older than After may leave, including leftover L0.
func Eligible(tier int, maxTs, now time.Time, after time.Duration) bool {
	if tier < 0 {
		return false
	}
	if after <= 0 {
		after = defaultAfter
	}
	if maxTs.IsZero() {
		return false
	}
	return !maxTs.After(now.Add(-after))
}

func (c *Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now().UTC()
}

func (c *Config) after() time.Duration {
	if c.After > 0 {
		return c.After
	}
	return defaultAfter
}

func (c *Config) maxTier() int {
	if c.MaxTier > 0 {
		return c.MaxTier
	}
	return 8
}

// Stats is the outcome of one Tenant pass.
type Stats struct {
	Attempts  int
	Successes int
	Retries   int
	Bytes     int64
	TmpFiles  int
}

// Tenant copies eligible L1+ files for one tenant from the hot root to cold.
// Each file is attempted once per call; a failure leaves the hot source in
// place so a later call retries. Empty ColdDir is a no-op.
func Tenant(c *Config, tenant string) (Stats, error) {
	var st Stats
	if c == nil || !Enabled(c.ColdDir) {
		return st, nil
	}
	st.TmpFiles = CountTemps(c.DataDir, c.ColdDir, tenant, c.maxTier())
	if err := GCTenant(c.DataDir, c.ColdDir, tenant, c.maxTier()); err != nil {
		return st, err
	}
	now := c.now()
	files, err := listHotCompacted(c.DataDir, tenant, c.maxTier())
	if err != nil {
		return st, err
	}
	var first error
	for _, f := range files {
		maxTs, ok := c.maxTs(f.Path)
		if !ok || !Eligible(f.Tier, maxTs, now, c.after()) {
			continue
		}
		st.Attempts++
		n, retried, err := promoteOne(c, tenant, f, now)
		if retried {
			st.Retries++
		}
		if err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		st.Successes++
		st.Bytes += n
	}
	return st, first
}

func (c *Config) maxTs(path string) (time.Time, bool) {
	if c.MaxTs != nil {
		return c.MaxTs(path)
	}
	return time.Time{}, false
}

type fileRef struct {
	Path string
	Rel  string
	Tier int
}

func listHotCompacted(dataDir, tenant string, maxTier int) ([]fileRef, error) {
	var out []fileRef
	for tier := 0; tier <= maxTier; tier++ {
		dir := layout.TierDir(dataDir, tenant, tier)
		files, err := listSegmentFiles(dir, filepath.ToSlash(filepath.Join("tiers", fmt.Sprintf("L%d", tier))), tier)
		if err != nil {
			return nil, err
		}
		out = append(out, files...)
	}
	artifacts, err := listLogArtifacts(dataDir, tenant)
	if err != nil {
		return nil, err
	}
	for _, artifact := range artifacts {
		for tier := 0; tier <= maxTier; tier++ {
			dir := layout.LogsTierDir(dataDir, tenant, artifact, tier)
			rel := filepath.ToSlash(filepath.Join("logs", artifact, "tiers", fmt.Sprintf("L%d", tier)))
			files, err := listSegmentFiles(dir, rel, tier)
			if err != nil {
				return nil, err
			}
			out = append(out, files...)
		}
	}
	return out, nil
}

func listLogArtifacts(dataDir, tenant string) ([]string, error) {
	root := filepath.Join(dataDir, tenant, "logs")
	entries, err := os.ReadDir(root)
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
	return out, nil
}

func listSegmentFiles(dir, relPrefix string, tier int) ([]fileRef, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	retired := layout.CompactedSet(entries)
	var out []fileRef
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || name == seed.SeedName || name[0] == '.' {
			continue
		}
		if layout.IsPromoteTemp(name) {
			continue
		}
		ext := filepath.Ext(name)
		if ext != ".parquet" && ext != ".duckdb" {
			continue
		}
		if _, held := retired[name]; held {
			continue
		}
		path := filepath.Join(dir, name)
		st, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		if segformat.TooSmall(st.Size()) {
			continue
		}
		out = append(out, fileRef{
			Path: path,
			Rel:  relPrefix + "/" + name,
			Tier: tier,
		})
	}
	return out, nil
}

func promoteOne(c *Config, tenant string, f fileRef, now time.Time) (int64, bool, error) {
	dest := filepath.Join(c.ColdDir, tenant, filepath.FromSlash(f.Rel))
	retried, err := recoverOrCopy(f.Path, dest)
	if err != nil {
		return 0, retried, err
	}
	fi, err := os.Stat(dest)
	if err != nil {
		return 0, retried, fmt.Errorf("promote: stat dest: %w", err)
	}
	if c.AfterPromote != nil {
		if err := c.AfterPromote(tenant); err != nil {
			return 0, retried, err
		}
	}
	if c.HoldSource != nil && c.Grace > 0 {
		return fi.Size(), retried, c.HoldSource(f.Path, now.Add(c.Grace))
	}
	if err := os.Remove(f.Path); err != nil && !os.IsNotExist(err) {
		return 0, retried, fmt.Errorf("promote: unlink source: %w", err)
	}
	return fi.Size(), retried, nil
}

func recoverOrCopy(src, dest string) (bool, error) {
	if fi, err := os.Lstat(dest); err == nil && fi.Mode().IsRegular() {
		srcSum, err := sha256File(src)
		if err != nil {
			return false, err
		}
		dstSum, err := sha256File(dest)
		if err != nil {
			return false, err
		}
		if srcSum == dstSum {
			return false, nil
		}
		if err := os.Remove(dest); err != nil {
			return true, fmt.Errorf("promote: remove broken dest: %w", err)
		}
		return true, CopyAtomic(src, dest)
	}
	return false, CopyAtomic(src, dest)
}
