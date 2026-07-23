// Package clickhouse drives schema setup, batched ingest, and queries for the
// store vs ClickHouse benchmark.
package clickhouse

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/elk-utilities/prism/bench/internal/gen"
)

const (
	defaultBatch = 50_000
)

// PinnedImage is the docker image tag recorded in benchmark docs.
const PinnedImage = "clickhouse/clickhouse-server:24.8"

// Config holds connection settings for the benchmark ClickHouse instance.
type Config struct {
	Addr string
}

// Client wraps a ClickHouse connection for benchmark workloads.
type Client struct {
	conn driver.Conn
}

// Open connects to ClickHouse at cfg.Addr (native protocol, port 9000).
func Open(cfg Config) (*Client, error) {
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Settings: clickhouse.Settings{
			"max_insert_block_size": defaultBatch,
		},
		DialTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}
	return &Client{conn: conn}, nil
}

// Close releases the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Version returns the server version string.
func (c *Client) Version(ctx context.Context) (string, error) {
	var v string
	if err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&v); err != nil {
		return "", fmt.Errorf("clickhouse: version: %w", err)
	}
	return v, nil
}

// InitSchema creates benchmark tables with tuned MergeTree settings.
func (c *Client) InitSchema(ctx context.Context) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS bench_metrics (
			__name__ LowCardinality(String),
			labels String,
			value Float64,
			timestamp_ms Int64,
			ts DateTime64(3, 'UTC')
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMMDD(ts)
		ORDER BY (ts, __name__)`,
		`CREATE TABLE IF NOT EXISTS bench_logs (
			ts DateTime64(3, 'UTC'),
			level LowCardinality(String),
			service LowCardinality(String),
			message String,
			INDEX idx_message message TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMMDD(ts)
		ORDER BY (ts, level, service)`,
	}
	for _, q := range ddl {
		if err := c.conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("clickhouse: ddl: %w", err)
		}
	}
	return nil
}

// Truncate clears benchmark tables between runs.
func (c *Client) Truncate(ctx context.Context) error {
	for _, tbl := range []string{"bench_metrics", "bench_logs"} {
		if err := c.conn.Exec(ctx, "TRUNCATE TABLE IF EXISTS "+tbl); err != nil {
			return fmt.Errorf("clickhouse: truncate %s: %w", tbl, err)
		}
	}
	return nil
}

// IngestMetrics inserts metrics rows in batches of defaultBatch.
func (c *Client) IngestMetrics(ctx context.Context, rows []gen.MetricRow) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO bench_metrics")
	if err != nil {
		return fmt.Errorf("clickhouse: prepare metrics: %w", err)
	}
	for i, r := range rows {
		if err := batch.Append(r.Name, r.Labels, r.Value, r.TimestampMs, r.Ts); err != nil {
			return fmt.Errorf("clickhouse: append metrics: %w", err)
		}
		if (i+1)%defaultBatch == 0 {
			if err := batch.Send(); err != nil {
				return fmt.Errorf("clickhouse: send metrics batch: %w", err)
			}
			batch, err = c.conn.PrepareBatch(ctx, "INSERT INTO bench_metrics")
			if err != nil {
				return fmt.Errorf("clickhouse: prepare metrics: %w", err)
			}
		}
	}
	if batch.Rows() > 0 {
		if err := batch.Send(); err != nil {
			return fmt.Errorf("clickhouse: send metrics tail: %w", err)
		}
	}
	return nil
}

// IngestLogs inserts log rows in batches.
func (c *Client) IngestLogs(ctx context.Context, rows []gen.LogRow) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO bench_logs")
	if err != nil {
		return fmt.Errorf("clickhouse: prepare logs: %w", err)
	}
	for i, r := range rows {
		if err := batch.Append(r.Ts, r.Level, r.Service, r.Message); err != nil {
			return fmt.Errorf("clickhouse: append logs: %w", err)
		}
		if (i+1)%defaultBatch == 0 {
			if err := batch.Send(); err != nil {
				return fmt.Errorf("clickhouse: send logs batch: %w", err)
			}
			batch, err = c.conn.PrepareBatch(ctx, "INSERT INTO bench_logs")
			if err != nil {
				return fmt.Errorf("clickhouse: prepare logs: %w", err)
			}
		}
	}
	if batch.Rows() > 0 {
		if err := batch.Send(); err != nil {
			return fmt.Errorf("clickhouse: send logs tail: %w", err)
		}
	}
	return nil
}

// CountMetrics returns row count in the ts range.
func (c *Client) CountMetrics(ctx context.Context, start, end time.Time) (int64, error) {
	var n uint64
	err := c.conn.QueryRow(ctx,
		"SELECT count() FROM bench_metrics WHERE ts >= ? AND ts < ?",
		start, end,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: count metrics: %w", err)
	}
	return int64(n), nil
}

// AggregateMetrics runs per-series avg/min/max/count over the range.
func (c *Client) AggregateMetrics(ctx context.Context, start, end time.Time) error {
	rows, err := c.conn.Query(ctx, `
		SELECT __name__, avg(value), min(value), max(value), count()
		FROM bench_metrics
		WHERE ts >= ? AND ts < ?
		GROUP BY __name__
	`, start, end)
	if err != nil {
		return fmt.Errorf("clickhouse: aggregate metrics: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		var avg, min, max float64
		var cnt uint64
		if err := rows.Scan(&name, &avg, &min, &max, &cnt); err != nil {
			return fmt.Errorf("clickhouse: scan aggregate: %w", err)
		}
	}
	return rows.Err()
}

// CountLogsLike returns rows matching the deadline substring in the range.
func (c *Client) CountLogsLike(ctx context.Context, start, end time.Time) (int64, error) {
	var n uint64
	err := c.conn.QueryRow(ctx, `
		SELECT count() FROM bench_logs
		WHERE ts >= ? AND ts < ?
		  AND message LIKE ?
	`, start, end, "%"+gen.DeadlineSubstring+"%").Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: logs like: %w", err)
	}
	return int64(n), nil
}

// WaitReady polls the HTTP /ping endpoint until ClickHouse responds or timeout.
func WaitReady(httpAddr string, timeout time.Duration) error {
	if httpAddr == "" {
		httpAddr = "http://127.0.0.1:8123"
	}
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, httpAddr+"/ping", nil)
		if err != nil {
			return fmt.Errorf("clickhouse: wait ready: %w", err)
		}
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return nil
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("clickhouse: not ready after %s", timeout)
}
