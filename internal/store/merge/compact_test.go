package merge

import (
	"testing"
	"time"
)

func catchupSpec() CompactSpec {
	return CompactSpec{
		Tier:       0,
		OlderThan:  15 * time.Minute,
		MaxSources: 32,
		MaxBytes:   256 << 20,
		SealBytes:  200,
		Bucket:     BucketNone,
	}
}

func TestSelectCompactFullyAgedIncludedYoungExcluded(t *testing.T) {
	now := fixtureBase.Add(time.Hour)
	aged := seg(0, "aged", 20, 0, 30*time.Minute)                // max_ts = base+30m ≤ now-15m
	young := seg(0, "young", 20, 50*time.Minute, 50*time.Minute) // max_ts = base+50m > now-15m
	other := seg(0, "other", 20, time.Minute, 20*time.Minute)
	action, ok := SelectCompact([]Segment{young, aged, other}, now, catchupSpec())
	if !ok {
		t.Fatal("want compact of fully-aged files")
	}
	if action.DestTier != 1 {
		t.Fatalf("dest tier = %d, want 1", action.DestTier)
	}
	if len(action.Sources) != 2 {
		t.Fatalf("sources = %d, want 2 aged files", len(action.Sources))
	}
	for _, s := range action.Sources {
		if s.Path == "young" {
			t.Fatal("young overlapping file must not be selected")
		}
	}
}

func TestSelectCompactNewerThanCutsOldSide(t *testing.T) {
	now := fixtureBase.Add(3 * time.Hour)
	tooOld := seg(0, "old", 20, 0, time.Minute) // max_ts well before now-1h
	inWin := seg(0, "a", 20, 2*time.Hour, 2*time.Hour+time.Minute)
	inWin2 := seg(0, "b", 20, 2*time.Hour+2*time.Minute, 2*time.Hour+3*time.Minute)
	spec := catchupSpec()
	spec.OlderThan = 15 * time.Minute
	spec.NewerThan = time.Hour
	action, ok := SelectCompact([]Segment{tooOld, inWin, inWin2}, now, spec)
	if !ok {
		t.Fatal("want rolling-window compact")
	}
	if len(action.Sources) != 2 {
		t.Fatalf("sources = %d, want 2 in-window", len(action.Sources))
	}
	for _, s := range action.Sources {
		if s.Path == "old" {
			t.Fatal("file older than newerThan must not be selected")
		}
	}
}

func TestSelectCompactFromToWindow(t *testing.T) {
	now := fixtureBase.Add(48 * time.Hour)
	from := fixtureBase.Add(24 * time.Hour)
	to := fixtureBase.Add(25 * time.Hour)
	inside := seg(0, "in", 20, 24*time.Hour, 24*time.Hour+time.Minute)
	inside2 := seg(0, "in2", 20, 24*time.Hour+2*time.Minute, 24*time.Hour+3*time.Minute)
	before := seg(0, "before", 20, 0, time.Minute)
	onTo := seg(0, "to", 20, 25*time.Hour, 25*time.Hour+time.Minute) // max_ts == to, excluded [from,to)
	spec := CompactSpec{
		Tier:       0,
		From:       from,
		To:         to,
		MaxSources: 32,
		MaxBytes:   256 << 20,
		SealBytes:  200,
		Bucket:     BucketNone,
	}
	action, ok := SelectCompact([]Segment{before, inside, inside2, onTo}, now, spec)
	if !ok {
		t.Fatal("want from/to compact")
	}
	if len(action.Sources) != 2 {
		t.Fatalf("sources = %d, want 2 inside [from,to)", len(action.Sources))
	}
}

func TestSelectCompactMaxSourcesShrinks(t *testing.T) {
	now := fixtureBase.Add(time.Hour)
	var segs []Segment
	for i := 0; i < 5; i++ {
		off := time.Duration(i) * 2 * time.Minute
		segs = append(segs, seg(0, pathID(i), 10, off, off+time.Minute))
	}
	spec := catchupSpec()
	spec.MaxSources = 3
	action, ok := SelectCompact(segs, now, spec)
	if !ok {
		t.Fatal("want compact")
	}
	if len(action.Sources) != 3 {
		t.Fatalf("sources = %d, want maxSources 3", len(action.Sources))
	}
	if action.Sources[0].Path != "a" || action.Sources[2].Path != "c" {
		t.Fatalf("want oldest-minTs prefix, got %+v", action.Sources)
	}
}

func TestSelectCompactMaxBytesStopsPrefix(t *testing.T) {
	now := fixtureBase.Add(time.Hour)
	segs := []Segment{
		seg(0, "a", 80, 0, time.Minute),
		seg(0, "b", 80, 2*time.Minute, 3*time.Minute),
		seg(0, "c", 80, 4*time.Minute, 5*time.Minute),
	}
	spec := catchupSpec()
	spec.MaxBytes = 160
	action, ok := SelectCompact(segs, now, spec)
	if !ok {
		t.Fatal("want compact of prefix that fits")
	}
	if len(action.Sources) != 2 {
		t.Fatalf("sources = %d, want 2 (third exceeds maxBytes)", len(action.Sources))
	}
	sum := int64(0)
	for _, s := range action.Sources {
		sum += s.Bytes
	}
	if sum > 160 {
		t.Fatalf("sum %d exceeds maxBytes", sum)
	}
}

func TestSelectCompactSealedExcluded(t *testing.T) {
	now := fixtureBase.Add(time.Hour)
	sealed := seg(0, "sealed", 200, 0, time.Minute)
	a := seg(0, "a", 20, 2*time.Minute, 3*time.Minute)
	b := seg(0, "b", 20, 4*time.Minute, 5*time.Minute)
	action, ok := SelectCompact([]Segment{sealed, a, b}, now, catchupSpec())
	if !ok {
		t.Fatal("want compact of unsealed pair")
	}
	for _, s := range action.Sources {
		if s.Path == "sealed" {
			t.Fatal("sealed file must not be a source")
		}
	}
	if len(action.Sources) != 2 {
		t.Fatalf("sources = %d, want 2", len(action.Sources))
	}
}

func TestSelectCompactFewerThanTwoNoAction(t *testing.T) {
	now := fixtureBase.Add(time.Hour)
	if _, ok := SelectCompact(nil, now, catchupSpec()); ok {
		t.Fatal("empty input must not compact")
	}
	one := []Segment{seg(0, "solo", 20, 0, time.Minute)}
	if _, ok := SelectCompact(one, now, catchupSpec()); ok {
		t.Fatal("single source must not compact")
	}
	wrongTier := []Segment{
		seg(1, "a", 20, 0, time.Minute),
		seg(1, "b", 20, 2*time.Minute, 3*time.Minute),
	}
	if _, ok := SelectCompact(wrongTier, now, catchupSpec()); ok {
		t.Fatal("tier mismatch must not compact")
	}
}

func TestSelectCompactBucketDayOldestFirst(t *testing.T) {
	now := fixtureBase.Add(72 * time.Hour)
	day0 := fixtureBase // 2026-01-01
	day1 := fixtureBase.Add(24 * time.Hour)
	segs := []Segment{
		seg(0, "d1a", 10, 24*time.Hour, 24*time.Hour+time.Minute),
		seg(0, "d1b", 10, 24*time.Hour+2*time.Minute, 24*time.Hour+3*time.Minute),
		seg(0, "d0a", 10, 0, time.Minute),
		seg(0, "d0b", 10, 2*time.Minute, 3*time.Minute),
		seg(0, "d0c", 10, 4*time.Minute, 5*time.Minute),
	}
	spec := CompactSpec{
		Tier:       0,
		OlderThan:  15 * time.Minute,
		MaxSources: 64,
		MaxBytes:   256 << 20,
		SealBytes:  200,
		Bucket:     BucketDay,
	}
	action, ok := SelectCompact(segs, now, spec)
	if !ok {
		t.Fatal("want day-bucket compact")
	}
	if len(action.Sources) != 3 {
		t.Fatalf("sources = %d, want 3 from oldest UTC day", len(action.Sources))
	}
	for _, s := range action.Sources {
		if s.MaxTs.UTC().Truncate(24 * time.Hour).Equal(day1.UTC().Truncate(24 * time.Hour)) {
			t.Fatalf("newer UTC day leaked into first pack: %s", s.Path)
		}
		if !s.MaxTs.UTC().Truncate(24 * time.Hour).Equal(day0.UTC().Truncate(24 * time.Hour)) {
			t.Fatalf("source %s not on oldest day", s.Path)
		}
	}
}

func TestSelectCompactBucketHourOldestFirst(t *testing.T) {
	now := fixtureBase.Add(5 * time.Hour)
	segs := []Segment{
		seg(0, "h1a", 10, time.Hour, time.Hour+time.Minute),
		seg(0, "h1b", 10, time.Hour+2*time.Minute, time.Hour+3*time.Minute),
		seg(0, "h0a", 10, 0, time.Minute),
		seg(0, "h0b", 10, 2*time.Minute, 3*time.Minute),
	}
	spec := CompactSpec{
		Tier:       0,
		OlderThan:  15 * time.Minute,
		MaxSources: 32,
		MaxBytes:   256 << 20,
		SealBytes:  200,
		Bucket:     BucketHour,
	}
	action, ok := SelectCompact(segs, now, spec)
	if !ok {
		t.Fatal("want hour-bucket compact")
	}
	if len(action.Sources) != 2 {
		t.Fatalf("sources = %d, want 2 from oldest UTC hour", len(action.Sources))
	}
	for _, s := range action.Sources {
		if s.Path[0:2] != "h0" {
			t.Fatalf("want oldest hour prefix, got %s", s.Path)
		}
	}
}

func TestSelectCompactBucketNeedsTwoInOldest(t *testing.T) {
	now := fixtureBase.Add(72 * time.Hour)
	segs := []Segment{
		seg(0, "lonely", 10, 0, time.Minute), // oldest day, only one file
		seg(0, "d1a", 10, 24*time.Hour, 24*time.Hour+time.Minute),
		seg(0, "d1b", 10, 24*time.Hour+2*time.Minute, 24*time.Hour+3*time.Minute),
	}
	spec := CompactSpec{
		Tier:       0,
		OlderThan:  15 * time.Minute,
		MaxSources: 64,
		MaxBytes:   256 << 20,
		SealBytes:  200,
		Bucket:     BucketDay,
	}
	action, ok := SelectCompact(segs, now, spec)
	if !ok {
		t.Fatal("want next UTC day that has ≥2 files")
	}
	if len(action.Sources) != 2 {
		t.Fatalf("sources = %d, want 2", len(action.Sources))
	}
	for _, s := range action.Sources {
		if s.Path == "lonely" {
			t.Fatal("singleton oldest day must be skipped")
		}
	}
}

func TestSelectCompactLargeAndSmallWhenSumFits(t *testing.T) {
	now := fixtureBase.Add(time.Hour)
	large := int64(215 << 20)
	small := int64(1 << 20)
	segs := []Segment{
		seg(0, "small", small, 0, time.Minute),
		seg(0, "large", large, 2*time.Minute, 3*time.Minute),
	}
	spec := catchupSpec()
	spec.SealBytes = 2 << 30
	spec.MaxBytes = 256 << 20
	action, ok := SelectCompact(segs, now, spec)
	if !ok {
		t.Fatal("large+small under maxBytes must pack together")
	}
	if len(action.Sources) != 2 {
		t.Fatalf("sources = %d, want both files", len(action.Sources))
	}
}

func TestSelectCompactSortedByMinTs(t *testing.T) {
	now := fixtureBase.Add(time.Hour)
	segs := []Segment{
		seg(0, "later", 10, 10*time.Minute, 11*time.Minute),
		seg(0, "earlier", 10, 0, time.Minute),
		seg(0, "mid", 10, 5*time.Minute, 6*time.Minute),
	}
	action, ok := SelectCompact(segs, now, catchupSpec())
	if !ok {
		t.Fatal("want compact")
	}
	want := []string{"earlier", "mid", "later"}
	if len(action.Sources) != 3 {
		t.Fatalf("sources = %d", len(action.Sources))
	}
	for i, s := range action.Sources {
		if s.Path != want[i] {
			t.Fatalf("sources[%d] = %s, want %s", i, s.Path, want[i])
		}
	}
}

func TestDefaultCatchupSpec(t *testing.T) {
	s := DefaultCatchupSpec()
	if s.Tier != 0 || s.OlderThan != 15*time.Minute || s.NewerThan != 0 {
		t.Fatalf("catch-up window = %+v", s)
	}
	if s.MaxSources != 32 || s.MaxBytes != 256<<20 {
		t.Fatalf("catch-up caps = sources %d bytes %d", s.MaxSources, s.MaxBytes)
	}
	if s.Bucket != BucketNone && s.Bucket != "" {
		t.Fatalf("catch-up bucket = %q, want none", s.Bucket)
	}
}
