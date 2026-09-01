package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
)

func TestSnapshotMissingTenant(t *testing.T) {
	t.Parallel()
	_, err := Snapshot(Options{
		DataDir: t.TempDir(),
		Tenant:  "user-missing-apps",
		Now:     time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC),
	})
	require.Error(t, err)
}

func TestSnapshotRejectsUnsafeTenant(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, tenant := range []string{"", "../etc", "a/b", "/abs"} {
		_, err := Snapshot(Options{DataDir: root, Tenant: tenant})
		require.Error(t, err, tenant)
	}
}

func TestSnapshotEmptyTenant(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	tenant := "user-empty-apps"
	require.NoError(t, os.MkdirAll(filepath.Join(root, tenant), 0o750))
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	snap, err := Snapshot(Options{DataDir: root, Tenant: tenant, Now: now})
	require.NoError(t, err)
	require.Equal(t, tenant, snap.Tenant)
	require.Equal(t, now.UTC(), snap.GeneratedAt.UTC())
	require.Equal(t, 0, snap.Totals.Files)
	require.Equal(t, int64(0), snap.Totals.Bytes)
	require.Empty(t, snap.Segments)
}

func TestSnapshotOrganizesDatesAndSizes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	tenant := "user-demo-apps"
	dayA := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	dayB := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)

	l0 := filepath.Join(root, tenant, "tiers", "L0", "1787086356308308631-aaaa.parquet")
	l1 := filepath.Join(root, tenant, "tiers", "L1", "1788260598192119595-bbbb.parquet")
	hot := filepath.Join(root, tenant, "hot", "current.parquet")
	landing := filepath.Join(root, tenant, "logs", "raw", "1788261489819083376-cccc.parquet")
	writeTSParquet(t, l0, []time.Time{dayA, dayA.Add(time.Hour)})
	writeTSParquet(t, l1, []time.Time{dayB})
	writeTSParquet(t, hot, []time.Time{dayB.Add(2 * time.Hour)})
	writeNSParquet(t, landing, []time.Time{dayB.Add(3 * time.Hour)})

	retired := filepath.Join(root, tenant, "tiers", "L0", "111-dead.parquet")
	writeTSParquet(t, retired, []time.Time{dayA})
	require.NoError(t, os.WriteFile(retired+".compacted", []byte("ok"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, tenant, "tiers", "L0", "scratch.parquet.tmp"), []byte("x"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, tenant, "tiers", "_manifest.json"), []byte("{}"), 0o600))

	snap, err := Snapshot(Options{
		DataDir: root,
		Tenant:  tenant,
		Now:     dayB.Add(4 * time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, 4, snap.Totals.Files)
	require.Greater(t, snap.Totals.Bytes, int64(0))
	require.Equal(t, 4, len(snap.Segments))

	require.Equal(t, 3, snap.ByFamily["metrics"].Files)
	require.Equal(t, 1, snap.ByFamily["logs"].Files)
	require.Equal(t, 1, snap.ByKind["hot"].Files)
	require.Equal(t, 1, snap.ByKind["landing"].Files)
	require.Equal(t, 2, snap.ByKind["tier"].Files)
	require.Equal(t, 1, snap.ByTier["L0"].Files)
	require.Equal(t, 1, snap.ByTier["L1"].Files)
	require.Equal(t, 4, snap.ByRoot["hot"].Files)

	var sawAug, sawSep Count
	for _, b := range snap.DateHistogram {
		switch b.Day {
		case "2026-08-18":
			sawAug = b.Count
		case "2026-09-01":
			sawSep = b.Count
		}
	}
	require.Equal(t, 1, sawAug.Files)
	require.Equal(t, 3, sawSep.Files)

	var sizeFiles int
	for _, b := range snap.SizeHistogram {
		sizeFiles += b.Files
	}
	require.Equal(t, 4, sizeFiles)

	var l0Seg *File
	for i := range snap.Segments {
		if snap.Segments[i].Rel == "tiers/L0/1787086356308308631-aaaa.parquet" {
			l0Seg = &snap.Segments[i]
		}
	}
	require.NotNil(t, l0Seg)
	require.Equal(t, "metrics", l0Seg.Family)
	require.Equal(t, 0, l0Seg.Tier)
	require.Equal(t, dayA.Unix(), l0Seg.MinTS.Unix())
}

func TestSnapshotColdRoot(t *testing.T) {
	t.Parallel()
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-cold-apps"
	require.NoError(t, os.MkdirAll(filepath.Join(hot, tenant), 0o750))
	day := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	writeTSParquet(t, filepath.Join(cold, tenant, "tiers", "L2", "100-cold.parquet"), []time.Time{day})

	snap, err := Snapshot(Options{DataDir: hot, ColdDir: cold, Tenant: tenant, Now: day})
	require.NoError(t, err)
	require.Equal(t, 1, snap.Totals.Files)
	require.Equal(t, 1, snap.ByRoot["cold"].Files)
	require.Equal(t, 1, snap.ByTier["L2"].Files)
	require.Equal(t, "cold", snap.Segments[0].Root)
}

func TestSnapshotUnreadableIsNotFatal(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	tenant := "user-bad-apps"
	bad := filepath.Join(root, tenant, "tiers", "L0", "1-bad.parquet")
	require.NoError(t, os.MkdirAll(filepath.Dir(bad), 0o750))
	require.NoError(t, os.WriteFile(bad, []byte("not parquet"), 0o600))
	snap, err := Snapshot(Options{DataDir: root, Tenant: tenant, Now: time.Now().UTC()})
	require.NoError(t, err)
	require.Equal(t, 1, snap.Totals.Files)
	require.Equal(t, 1, snap.Totals.Unreadable)
	require.Equal(t, 1, len(snap.Segments))
	require.NotEmpty(t, snap.Segments[0].BoundsErr)
}

func TestSnapshotJSON(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	tenant := "user-json-apps"
	require.NoError(t, os.MkdirAll(filepath.Join(root, tenant), 0o750))
	snap, err := Snapshot(Options{DataDir: root, Tenant: tenant, Now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)})
	require.NoError(t, err)
	raw, err := json.Marshal(snap)
	require.NoError(t, err)
	var round Report
	require.NoError(t, json.Unmarshal(raw, &round))
	require.Equal(t, tenant, round.Tenant)
	require.Contains(t, string(raw), `"date_histogram"`)
	require.Contains(t, string(raw), `"size_histogram"`)
}

func TestRunCLIWritesJSON(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	tenant := "user-cli-apps"
	require.NoError(t, os.MkdirAll(filepath.Join(root, tenant), 0o750))
	var out bytes.Buffer
	err := run(nil, &out, []string{
		"--data-dir", root,
		"--tenant", tenant,
	})
	require.NoError(t, err)
	var snap Report
	require.NoError(t, json.Unmarshal(out.Bytes(), &snap))
	require.Equal(t, tenant, snap.Tenant)
	require.Empty(t, snap.Segments)
}

func TestRunCLIListIncludesSegments(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	tenant := "user-list-apps"
	writeTSParquet(t, filepath.Join(root, tenant, "tiers", "L0", "1-a.parquet"), []time.Time{
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	var out bytes.Buffer
	require.NoError(t, run(nil, &out, []string{"--data-dir", root, "--tenant", tenant, "--list"}))
	var snap Report
	require.NoError(t, json.Unmarshal(out.Bytes(), &snap))
	require.Equal(t, 1, len(snap.Segments))
}

func TestRunCLIRequiresTenant(t *testing.T) {
	t.Parallel()
	err := run(nil, bytes.NewBuffer(nil), []string{"--data-dir", t.TempDir()})
	require.Error(t, err)
}

func writeTSParquet(t *testing.T, path string, ts []time.Time) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
	}, nil)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	b := array.NewTimestampBuilder(mem, schema.Field(0).Type.(*arrow.TimestampType))
	defer b.Release()
	for _, tm := range ts {
		b.Append(arrow.Timestamp(tm.UTC().UnixNano()))
	}
	col := b.NewArray()
	defer col.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{col}, int64(len(ts)))
	defer rec.Release()
	writeRecord(t, path, rec)
}

func writeNSParquet(t *testing.T, path string, ts []time.Time) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "__prism_ts_ns", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, tm := range ts {
		b.Append(tm.UTC().UnixNano())
	}
	col := b.NewArray()
	defer col.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{col}, int64(len(ts)))
	defer rec.Release()
	writeRecord(t, path, rec)
}

func writeRecord(t *testing.T, path string, rec arrow.RecordBatch) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	w, err := pqarrow.NewFileWriter(rec.Schema(), f, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())
}
