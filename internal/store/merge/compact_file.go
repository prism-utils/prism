package merge

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/config"
	"gopkg.in/yaml.v3"
)

var compactNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// Policy is one named compact window from COMPACT_FILE.
type Policy struct {
	Name       string `yaml:"name"`
	Tier       int    `yaml:"tier"`
	OlderThan  string `yaml:"olderThan"`
	NewerThan  string `yaml:"newerThan"`
	Bucket     string `yaml:"bucket"`
	MaxSources int    `yaml:"maxSources"`
	MaxBytes   string `yaml:"maxBytes"`
	Every      string `yaml:"every"`

	every time.Duration
}

// File is the loaded compact document. Empty path yields no policies.
type File struct {
	Policies []Policy
}

type compactDoc struct {
	Compact struct {
		Policies []Policy `yaml:"policies"`
	} `yaml:"compact"`
}

// LoadCompact reads YAML named policies. An empty path is a no-op.
func LoadCompact(path string) (File, error) {
	if strings.TrimSpace(path) == "" {
		return File{}, nil
	}
	body, err := os.ReadFile(path) //nolint:gosec // G304: operator-supplied config path
	if err != nil {
		return File{}, fmt.Errorf("compact: read %s: %w", path, err)
	}
	var doc compactDoc
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return File{}, fmt.Errorf("compact: parse %s: %w", path, err)
	}
	return File{Policies: doc.Compact.Policies}, nil
}

// Lookup returns a named policy.
func (f File) Lookup(name string) (Policy, bool) {
	for i := range f.Policies {
		if f.Policies[i].Name == name {
			return f.Policies[i], true
		}
	}
	return Policy{}, false
}

// Validate rejects invalid names, durations, duplicate names, and cap violations.
func (f *File) Validate(maxSegmentBytes int64) error {
	seen := make(map[string]struct{}, len(f.Policies))
	for i := range f.Policies {
		if err := f.Policies[i].compile(i, maxSegmentBytes); err != nil {
			return err
		}
		name := f.Policies[i].Name
		if _, dup := seen[name]; dup {
			return fmt.Errorf("policies[%d].name: duplicate %q", i, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// Interval is how long to wait between runs of this policy for one tenant.
func (p *Policy) Interval() time.Duration {
	if p.every != 0 {
		return p.every
	}
	d, _ := parseCompactDuration(p.Every)
	return d
}

// Spec returns the selector for this policy.
func (p *Policy) Spec() (CompactSpec, error) {
	return p.parseSpec()
}

func (p *Policy) compile(i int, maxSegmentBytes int64) error {
	if !compactNameRe.MatchString(p.Name) {
		return fmt.Errorf("policies[%d].name: %q must match %s", i, p.Name, compactNameRe.String())
	}
	if strings.Contains(p.Name, "..") || strings.ContainsAny(p.Name, `/\`) {
		return fmt.Errorf("policies[%d].name: path traversal is not allowed", i)
	}
	if p.MaxSources < 2 {
		return fmt.Errorf("policies[%d].maxSources: must be >= 2", i)
	}
	spec, err := p.parseSpec()
	if err != nil {
		return fmt.Errorf("policies[%d]: %w", i, err)
	}
	if spec.MaxBytes <= 0 {
		return fmt.Errorf("policies[%d].maxBytes: must be > 0", i)
	}
	if maxSegmentBytes > 0 && spec.MaxBytes > maxSegmentBytes {
		return fmt.Errorf("policies[%d].maxBytes: must be <= %d", i, maxSegmentBytes)
	}
	every, err := parseCompactDuration(p.Every)
	if err != nil {
		return fmt.Errorf("policies[%d].every: %w", i, err)
	}
	if every <= 0 {
		return fmt.Errorf("policies[%d].every: must be > 0", i)
	}
	p.every = every
	return nil
}

func (p *Policy) parseSpec() (CompactSpec, error) {
	older, err := parseCompactDuration(p.OlderThan)
	if err != nil {
		return CompactSpec{}, fmt.Errorf("olderThan: %w", err)
	}
	newer, err := parseCompactDuration(p.NewerThan)
	if err != nil {
		return CompactSpec{}, fmt.Errorf("newerThan: %w", err)
	}
	bucket, err := parseCompactBucket(p.Bucket)
	if err != nil {
		return CompactSpec{}, err
	}
	var maxBytes int64
	if strings.TrimSpace(p.MaxBytes) != "" {
		maxBytes, err = config.ParseByteSize(p.MaxBytes)
		if err != nil {
			return CompactSpec{}, fmt.Errorf("maxBytes: %w", err)
		}
	}
	return CompactSpec{
		Tier:       p.Tier,
		OlderThan:  older,
		NewerThan:  newer,
		Bucket:     bucket,
		MaxSources: p.MaxSources,
		MaxBytes:   maxBytes,
	}, nil
}

func parseCompactBucket(s string) (Bucket, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", string(BucketNone):
		return BucketNone, nil
	case string(BucketHour):
		return BucketHour, nil
	case string(BucketDay):
		return BucketDay, nil
	default:
		return "", fmt.Errorf("bucket: %q must be none, hour, or day", s)
	}
}

func parseCompactDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if n, ok := parseDayCount(s); ok {
		if n < 0 {
			return 0, fmt.Errorf("duration %q: must be >= 0", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("duration %q: %w", s, err)
	}
	return d, nil
}

func parseDayCount(s string) (int, bool) {
	if !strings.HasSuffix(s, "d") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
	if err != nil {
		return 0, false
	}
	return n, true
}
