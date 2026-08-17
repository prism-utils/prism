package metrics

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Closed DuckDB role labels for prism_store_duckdb_open.
const (
	RoleEngine      = "engine"
	RoleMerge       = "merge"
	RoleRollup      = "rollup"
	RoleMaterialize = "materialize"
	RoleSQL         = "sql"
	RolePromQL      = "promql"
	RoleLoki        = "loki"
	RoleBounds      = "bounds"
	RoleStat        = "stat"
)

const defaultCgroupRoot = "/sys/fs/cgroup"

var duckdbRoles = []string{
	RoleEngine,
	RoleMerge,
	RoleRollup,
	RoleMaterialize,
	RoleSQL,
	RolePromQL,
	RoleLoki,
	RoleBounds,
	RoleStat,
}

var (
	observeArmed atomic.Bool
	duckdbOpen   [9]atomic.Int64
)

var (
	observeEnabledDesc = prometheus.NewDesc(
		namespace+"_memory_observe",
		"1 when extra memory-debug series are registered.",
		nil, nil)
	cgroupMemoryDesc = prometheus.NewDesc(
		namespace+"_cgroup_memory_bytes",
		"cgroup v2 memory interface files for this process.",
		[]string{"kind"}, nil)
	goMemLimitDesc = prometheus.NewDesc(
		namespace+"_gomemlimit_bytes",
		"Configured Go memory limit; 0 if unset.",
		nil, nil)
	duckdbMemLimitDesc = prometheus.NewDesc(
		namespace+"_duckdb_memory_limit_bytes",
		"Configured DuckDB memory_limit; 0 if unset.",
		nil, nil)
	duckdbOpenDesc = prometheus.NewDesc(
		namespace+"_duckdb_open",
		"Live DuckDB instances by role.",
		[]string{"role"}, nil)
)

func roleIndex(role string) (int, bool) {
	for i, r := range duckdbRoles {
		if r == role {
			return i, true
		}
	}
	return 0, false
}

// DuckDBOpen records one live instance. When observation is off this is a
// single atomic load and returns without taking a lock.
func DuckDBOpen(role string) {
	if !observeArmed.Load() {
		return
	}
	i, ok := roleIndex(role)
	if !ok {
		return
	}
	duckdbOpen[i].Add(1)
}

// DuckDBClose records one instance close. Unknown roles and a disabled flag
// are ignored; the counter never goes below zero.
func DuckDBClose(role string) {
	if !observeArmed.Load() {
		return
	}
	i, ok := roleIndex(role)
	if !ok {
		return
	}
	for {
		v := duckdbOpen[i].Load()
		if v <= 0 {
			return
		}
		if duckdbOpen[i].CompareAndSwap(v, v-1) {
			return
		}
	}
}

// ResetObserveForTest clears the process-wide observe arming and counters.
func ResetObserveForTest() {
	observeArmed.Store(false)
	for i := range duckdbOpen {
		duckdbOpen[i].Store(0)
	}
}

func (r *Registry) buildObserve() {
	r.observe = true
	r.goMemLimit = float64(r.cfg.GoMemLimitBytes)
	r.duckdbLimit = float64(r.cfg.DuckDBMemoryLimitBytes)
	r.cgroupRoot = strings.TrimSpace(r.cfg.CgroupRoot)
	if r.cgroupRoot == "" {
		r.cgroupRoot = defaultCgroupRoot
	}
	for i := range duckdbOpen {
		duckdbOpen[i].Store(0)
	}
	observeArmed.Store(true)

	r.jobRSS = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_rss_bytes",
		Help:      "Process RSS at the start or end of the last lifecycle pass.",
	}, []string{"job", "phase"})
	r.jobCgroup = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_cgroup_current_bytes",
		Help:      "cgroup memory.current at the start or end of the last lifecycle pass.",
	}, []string{"job", "phase"})
	r.jobHeap = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "job_heap_alloc_bytes",
		Help:      "Go heap allocation at the start or end of the last lifecycle pass.",
	}, []string{"job", "phase"})
}

type observeCollector struct {
	r *Registry
}

func (c *observeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- observeEnabledDesc
	ch <- cgroupMemoryDesc
	ch <- goMemLimitDesc
	ch <- duckdbMemLimitDesc
	ch <- duckdbOpenDesc
}

func (c *observeCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(observeEnabledDesc, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(goMemLimitDesc, prometheus.GaugeValue, c.r.goMemLimit)
	ch <- prometheus.MustNewConstMetric(duckdbMemLimitDesc, prometheus.GaugeValue, c.r.duckdbLimit)
	for i, role := range duckdbRoles {
		ch <- prometheus.MustNewConstMetric(duckdbOpenDesc, prometheus.GaugeValue, float64(duckdbOpen[i].Load()), role)
	}
	for _, kind := range []string{"current", "peak", "max"} {
		v, ok := readCgroupBytes(c.r.cgroupRoot, "memory."+kind)
		if !ok {
			continue
		}
		ch <- prometheus.MustNewConstMetric(cgroupMemoryDesc, prometheus.GaugeValue, v, kind)
	}
}

type memSample struct {
	rss   float64
	rssOK bool
	heap  float64
	cg    float64
	cgOK  bool
}

func sampleMem(cgroupRoot string) memSample {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s := memSample{heap: float64(ms.HeapAlloc)}
	if rss, ok := readRSSBytes(); ok {
		s.rss, s.rssOK = rss, true
	}
	if cg, ok := readCgroupBytes(cgroupRoot, "memory.current"); ok {
		s.cg, s.cgOK = cg, true
	}
	return s
}

func readCgroupBytes(root, name string) (float64, bool) {
	body, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		return 0, false
	}
	text := strings.TrimSpace(string(body))
	if text == "" || strings.EqualFold(text, "max") {
		return 0, false
	}
	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, false
	}
	return float64(n), true
}

func readRSSBytes() (float64, bool) {
	body, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(body))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	page := os.Getpagesize()
	if page <= 0 {
		return 0, false
	}
	return float64(pages) * float64(page), true
}
