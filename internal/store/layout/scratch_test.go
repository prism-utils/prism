package layout

import "testing"

func TestIsHotScratchAllowlist(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want bool
	}{
		{name: "current.parquet.deadbeef.tmp", want: true},
		{name: "current.duckdb.deadbeef.tmp", want: true},
		{name: "current.duckdb.deadbeef.tmp.wal", want: true},
		{name: "current.parquet.abcd0123.tmp.wal", want: true},
		{name: ".read-deadbeef.duckdb", want: true},
		{name: ".read-cafebabe.parquet", want: true},
		{name: "current.parquet", want: false},
		{name: "current.duckdb", want: false},
		{name: "current.duckdb.wal", want: false},
		{name: "orphan.tmp", want: false},
		{name: "keep.parquet.tmp", want: false},
		{name: "current.parquet.tmp", want: false},
		{name: "current.duckdb.not-hex.tmp", want: false},
		{name: "seg.parquet.aaaaaaaa.promote.tmp", want: false},
		{name: ".read-", want: false},
		{name: "engine.duckdb.tmp", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsHotScratch(tc.name); got != tc.want {
				t.Fatalf("IsHotScratch(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestIsEngineScratchAllowlist(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want bool
	}{
		{name: "engine.duckdb.tmp", want: true},
		{name: "engine.duckdb", want: false},
		{name: "engine.duckdb.wal", want: false},
		{name: "engine.duckdb.tmp.wal", want: false},
		{name: "orphan.tmp", want: false},
		{name: "current.duckdb.tmp", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsEngineScratch(tc.name); got != tc.want {
				t.Fatalf("IsEngineScratch(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestIsMaterializeScratchAllowlist(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want bool
	}{
		{name: "seg.parquet.tmp", want: true},
		{name: "seg.duckdb.tmp", want: true},
		{name: "keep.parquet.tmp", want: true},
		{name: "keep.duckdb.tmp", want: true},
		{name: "seg.parquet", want: false},
		{name: "seg.duckdb", want: false},
		{name: "current.parquet", want: false},
		{name: "current.duckdb", want: false},
		{name: "current.parquet.deadbeef.tmp", want: false},
		{name: "current.duckdb.deadbeef.tmp", want: false},
		{name: "orphan.tmp", want: false},
		{name: "seg.parquet.aaaaaaaa.promote.tmp", want: false},
		{name: "engine.duckdb.tmp", want: false},
		{name: ".read-deadbeef.duckdb", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsMaterializeScratch(tc.name); got != tc.want {
				t.Fatalf("IsMaterializeScratch(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}
