package engine

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenTenant_unsetThreadsPreservesDefault(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	te, err := openTenant(dir, testTenant, Config{DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = te.db.Close() })

	var threads string
	err = te.db.QueryRowContext(context.Background(), "SELECT current_setting('threads')").Scan(&threads)
	require.NoError(t, err)
	n, err := strconv.Atoi(threads)
	require.NoError(t, err)
	require.Positive(t, n, "unset config should leave DuckDB default threads")
}

func TestOpenTenant_appliedThreadsSetting(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const want = 2
	te, err := openTenant(dir, testTenant, Config{DataDir: dir, Threads: want})
	require.NoError(t, err)
	t.Cleanup(func() { _ = te.db.Close() })

	var threads string
	err = te.db.QueryRowContext(context.Background(), "SELECT current_setting('threads')").Scan(&threads)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(want), threads)
}

func TestEngineWithThreads_appliedAtOpen(t *testing.T) {
	t.Parallel()
	start := testNow(t)
	e := New(Config{DataDir: t.TempDir(), HotWindow: time.Minute, Threads: 3}, start)
	t.Cleanup(func() { _ = e.Close() })

	var threads string
	err := e.WithRead(testTenant, func(db *sql.DB) error {
		return db.QueryRowContext(context.Background(), "SELECT current_setting('threads')").Scan(&threads)
	})
	require.NoError(t, err)
	require.Equal(t, "3", threads)
}

func testNow(t *testing.T) func() time.Time {
	t.Helper()
	clk := testClock(t)
	return func() time.Time { return clk() }
}

func testClock(t *testing.T) func() time.Time {
	t.Helper()
	start := time.Unix(1700000000, 0).UTC()
	return func() time.Time { return start }
}
