//go:build !cgo

package store

import (
	"fmt"
)

func newDriver(cfg Config) (Driver, error) {
	return nil, fmt.Errorf("store: benchmark driver requires CGO_ENABLED=1 (DuckDB)")
}
