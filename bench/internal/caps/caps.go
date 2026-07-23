package caps

import (
	"fmt"
	"strconv"
)

const (
	// DefaultCPUs is the default vCPU cap per system in the benchmark harness.
	DefaultCPUs = 2
	// DefaultMemMiB is the default memory cap per system in mebibytes.
	DefaultMemMiB = 1024
)

// Budget is the equal per-system CPU and memory envelope for the benchmark.
type Budget struct {
	CPUs   float64
	MemMiB int
}

// DefaultBudget returns the harness default (2 vCPU / 1 GiB per system).
func DefaultBudget() Budget {
	return Budget{CPUs: DefaultCPUs, MemMiB: DefaultMemMiB}
}

// IsSet reports whether an explicit resource budget was configured.
func (b Budget) IsSet() bool {
	return b.CPUs > 0 || b.MemMiB > 0
}

// DuckDBMemoryLimit formats mem MiB for DuckDB SET memory_limit (empty when unset).
func (b Budget) DuckDBMemoryLimit() string {
	if b.MemMiB <= 0 {
		return ""
	}
	return fmt.Sprintf("%dMB", b.MemMiB)
}

// GoMemLimit formats mem for the Go runtime GOMEMLIMIT variable.
func (b Budget) GoMemLimit() string {
	return fmt.Sprintf("%dMiB", b.MemMiB)
}

// ComposeMemLimit returns a Docker Compose mem_limit value.
func (b Budget) ComposeMemLimit() string {
	if b.MemMiB%1024 == 0 {
		return fmt.Sprintf("%dg", b.MemMiB/1024)
	}
	return fmt.Sprintf("%dm", b.MemMiB)
}

// ComposeCPUs returns a Docker Compose cpus string.
func (b Budget) ComposeCPUs() string {
	return strconv.FormatFloat(b.CPs(), 'f', -1, 64)
}

// CPs is the integer thread count derived from the CPU budget.
func (b Budget) CPs() float64 {
	if b.CPUs <= 0 {
		return DefaultCPUs
	}
	return b.CPUs
}

// Threads is the DuckDB / GOMAXPROCS thread count (zero when budget unset).
func (b Budget) Threads() int {
	if !b.IsSet() {
		return 0
	}
	n := int(b.CPs())
	if n <= 0 {
		return DefaultCPUs
	}
	return n
}

// ClickHouseMaxMemoryBytes returns per-query max memory for ClickHouse settings.
func (b Budget) ClickHouseMaxMemoryBytes() uint64 {
	miB := b.MemMiB
	if miB <= 0 {
		miB = DefaultMemMiB
	}
	return uint64(miB) * 1024 * 1024
}

// ClickHouseServerMaxMemoryBytes returns server-wide memory cap (slightly below compose limit).
func (b Budget) ClickHouseServerMaxMemoryBytes() uint64 {
	return b.ClickHouseMaxMemoryBytes() * 9 / 10
}
