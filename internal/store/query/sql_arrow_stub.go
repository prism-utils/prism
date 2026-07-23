//go:build !duckdb_arrow

package query

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
)

func writeArrowResponse(_ context.Context, w http.ResponseWriter, _ *sql.Conn, _ string, _ int, _ *slog.Logger) error {
	http.Error(w, "arrow transport unavailable", http.StatusNotAcceptable)
	return nil
}
