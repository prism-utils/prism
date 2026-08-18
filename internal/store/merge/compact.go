package merge

import (
	"sort"
	"time"
)

// Bucket is a UTC calendar grouping for compact sources.
type Bucket string

const (
	// BucketNone packs any eligible files in min-ts order.
	BucketNone Bucket = "none"
	// BucketHour keeps only the oldest UTC hour that still has two or more files.
	BucketHour Bucket = "hour"
	// BucketDay keeps only the oldest UTC day that still has two or more files.
	BucketDay Bucket = "day"
)

const (
	// DefaultCatchupOlderThan is how old a file's max timestamp must be before
	// catch-up will consider it.
	DefaultCatchupOlderThan = 15 * time.Minute
	// DefaultCatchupMaxSources caps how many files one catch-up pack may take.
	DefaultCatchupMaxSources = 32
	// DefaultCatchupMaxBytes caps the summed size of one catch-up pack (256 MiB).
	DefaultCatchupMaxBytes = 256 << 20
)

// CompactSpec selects a bounded set of fully-aged segments to merge.
type CompactSpec struct {
	Tier       int
	OlderThan  time.Duration
	NewerThan  time.Duration
	From       time.Time
	To         time.Time
	Bucket     Bucket
	MaxSources int
	MaxBytes   int64
	// SealBytes is the size at which a file is finished and must not be a
	// compact source. Zero disables that filter.
	SealBytes int64
}

// DefaultCatchupSpec is the built-in age packer: tier 0, 15 minutes old, 32
// files or 256 MiB, no rolling upper bound.
func DefaultCatchupSpec() CompactSpec {
	return CompactSpec{
		Tier:       0,
		OlderThan:  DefaultCatchupOlderThan,
		MaxSources: DefaultCatchupMaxSources,
		MaxBytes:   DefaultCatchupMaxBytes,
		Bucket:     BucketNone,
	}
}

// SelectCompact returns one merge into the next tier, or false when fewer than
// two eligible sources fit the window and caps.
func SelectCompact(segments []Segment, now time.Time, spec CompactSpec) (MergeAction, bool) { //nolint:gocritic // selector bag copied once per plan
	if spec.MaxSources < 2 || spec.MaxBytes <= 0 {
		return MergeAction{}, false
	}
	eligible := make([]Segment, 0, len(segments))
	for i := range segments {
		if !compactEligible(&segments[i], now, &spec) {
			continue
		}
		eligible = append(eligible, segments[i])
	}
	eligible = filterOldestBucket(eligible, spec.Bucket)
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].MinTs.Equal(eligible[j].MinTs) {
			return eligible[i].Path < eligible[j].Path
		}
		return eligible[i].MinTs.Before(eligible[j].MinTs)
	})
	picked := takePrefix(eligible, spec.MaxSources, spec.MaxBytes)
	if len(picked) < 2 {
		return MergeAction{}, false
	}
	return MergeAction{Sources: picked, DestTier: spec.Tier + 1}, true
}

func compactEligible(s *Segment, now time.Time, spec *CompactSpec) bool {
	if s.Tier != spec.Tier {
		return false
	}
	if spec.SealBytes > 0 && s.Bytes >= spec.SealBytes {
		return false
	}
	if spec.OlderThan > 0 && s.MaxTs.After(now.Add(-spec.OlderThan)) {
		return false
	}
	if spec.NewerThan > 0 && !s.MaxTs.After(now.Add(-spec.NewerThan)) {
		return false
	}
	if !spec.From.IsZero() && s.MaxTs.Before(spec.From) {
		return false
	}
	if !spec.To.IsZero() && !s.MaxTs.Before(spec.To) {
		return false
	}
	return true
}

func filterOldestBucket(segs []Segment, bucket Bucket) []Segment {
	if bucket != BucketHour && bucket != BucketDay {
		return segs
	}
	groups := map[time.Time][]Segment{}
	keys := make([]time.Time, 0)
	seen := map[time.Time]struct{}{}
	for i := range segs {
		k := utcBucket(segs[i].MaxTs, bucket)
		if _, ok := seen[k]; !ok {
			keys = append(keys, k)
			seen[k] = struct{}{}
		}
		groups[k] = append(groups[k], segs[i])
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Before(keys[j]) })
	for _, k := range keys {
		if len(groups[k]) >= 2 {
			return groups[k]
		}
	}
	return nil
}

func utcBucket(ts time.Time, bucket Bucket) time.Time {
	ts = ts.UTC()
	switch bucket {
	case BucketHour:
		return time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), 0, 0, 0, time.UTC)
	default:
		return time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
	}
}

func takePrefix(sorted []Segment, maxSources int, maxBytes int64) []Segment {
	picked := make([]Segment, 0, min(maxSources, len(sorted)))
	var sum int64
	for i := range sorted {
		if len(picked) >= maxSources {
			break
		}
		if sum+sorted[i].Bytes > maxBytes {
			break
		}
		picked = append(picked, sorted[i])
		sum += sorted[i].Bytes
	}
	return picked
}
