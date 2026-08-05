package query

// sandboxOpenSetThreadCap is the open-set size above which query sandboxes fall
// back to a single DuckDB thread to limit planning memory.
const sandboxOpenSetThreadCap = 500

// EffectiveSandboxThreads returns the thread count applied to a query sandbox.
// When the Parquet open set exceeds sandboxOpenSetThreadCap, configured threads
// are reduced to 1 regardless of the operator setting.
func EffectiveSandboxThreads(configured, openSetSize int) int {
	if configured <= 0 {
		return 0
	}
	if openSetSize > sandboxOpenSetThreadCap {
		return 1
	}
	return configured
}

// lastAppliedSandboxThreads records the most recently applied sandbox thread count.
var lastAppliedSandboxThreads int

// AppliedSandboxThreadsForTest returns the thread count from the last sandbox open.
func AppliedSandboxThreadsForTest() int {
	return lastAppliedSandboxThreads
}

func recordAppliedSandboxThreads(n int) {
	lastAppliedSandboxThreads = n
}
