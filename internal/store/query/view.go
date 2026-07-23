package query

import "fmt"

// ViewSQL emits CREATE OR REPLACE VIEW … for Grafana DuckDB initSQL wiring.
func ViewSQL(dataDir, tenant string) (string, error) {
	return "", fmt.Errorf("query: not implemented")
}
