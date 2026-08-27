package query

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
)

const (
	queryLogCap     = 512
	queryErrBodyCap = 1024
)

type queryErrorBody struct {
	Error string `json:"error"`
}

// truncateQuery caps SQL/LogQL in logs so a Grafana search box cannot dump
// unbounded lines (PII, cost). n is a byte budget.
func truncateQuery(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n]
}

func writeJSONError(w http.ResponseWriter, logger *slog.Logger, ns string, status int, msg string) {
	payload, err := json.Marshal(queryErrorBody{Error: truncateQuery(msg, queryErrBodyCap)})
	if err != nil {
		if logger != nil {
			logger.Error("query error encode", "ns", ns, "err", err)
		}
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(payload); err != nil && logger != nil {
		logger.Error("query error write", "ns", ns, "err", err)
	}
}

func logQueryFailure(ctx context.Context, logger *slog.Logger, level slog.Level, msg, ns, queryKey, queryText string, status int, err error) {
	if logger == nil {
		return
	}
	logger.Log(ctx, level, msg,
		"ns", ns,
		"status", status,
		queryKey, truncateQuery(queryText, queryLogCap),
		"err", err,
	)
}
