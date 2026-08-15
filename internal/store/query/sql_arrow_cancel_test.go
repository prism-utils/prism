//go:build duckdb_arrow

package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/query"
)

func TestSQLArrowClientCancelReturns499(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	h := sqlHandler(t, dataDir, sqlConfig(dataDir, func(c *query.SQLConfig) {
		c.Timeout = 30 * time.Second
	}), eng)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := serveSQL(t, h, ctx, tenantSQLA, "SELECT 1", arrowStreamAccept)
	assertClientClosed(t, rec)
}
