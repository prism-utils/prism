package merge

import "time"

// Segment describes one on-disk parquet file in the tiered store.
type Segment struct {
	Tier  int
	Path  string
	Bytes int64
	MinTs time.Time
	MaxTs time.Time
}

// MergeAction merges Sources into DestTier.
type MergeAction struct {
	Sources  []Segment
	DestTier int
}

// DeleteAction removes an expired segment.
type DeleteAction struct {
	Segment Segment
}
