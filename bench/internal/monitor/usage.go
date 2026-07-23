// Package monitor samples CPU, memory, and disk I/O for benchmark workloads.
package monitor

// Usage holds aggregated resource consumption over a sampling window.
type Usage struct {
	CPUCoresMean float64 `json:"cpu_cores_mean"`
	CPUCoresPeak float64 `json:"cpu_cores_peak"`
	RSSPeakBytes uint64  `json:"rss_peak_bytes"`
	ReadBytes    uint64  `json:"read_bytes"`
	WriteBytes   uint64  `json:"write_bytes"`
	ReadOps      *uint64 `json:"read_ops,omitempty"`
	WriteOps     *uint64 `json:"write_ops,omitempty"`
	DurationSec  float64 `json:"duration_sec"`
}

// RSSPeakMiB returns peak resident set size in mebibytes.
func (u Usage) RSSPeakMiB() float64 {
	return float64(u.RSSPeakBytes) / (1024 * 1024)
}

// ReadMiBPerSec returns read throughput in MiB/s over the sampling window.
func (u Usage) ReadMiBPerSec() float64 {
	if u.DurationSec <= 0 {
		return 0
	}
	return float64(u.ReadBytes) / (1024 * 1024) / u.DurationSec
}

// WriteMiBPerSec returns write throughput in MiB/s over the sampling window.
func (u Usage) WriteMiBPerSec() float64 {
	if u.DurationSec <= 0 {
		return 0
	}
	return float64(u.WriteBytes) / (1024 * 1024) / u.DurationSec
}

// TotalReadWriteMiB returns combined read+write volume in MiB.
func (u Usage) TotalReadWriteMiB() float64 {
	return float64(u.ReadBytes+u.WriteBytes) / (1024 * 1024)
}

// TotalMiBPerSec returns combined read+write throughput in MiB/s.
func (u Usage) TotalMiBPerSec() float64 {
	return u.ReadMiBPerSec() + u.WriteMiBPerSec()
}

// IOPS returns combined read+write operations per second when op counts exist.
func (u Usage) IOPS() (float64, bool) {
	if u.ReadOps == nil || u.WriteOps == nil || u.DurationSec <= 0 {
		return 0, false
	}
	return float64(*u.ReadOps+*u.WriteOps) / u.DurationSec, true
}

// IOAvailable reports whether per-operation disk counters were collected.
func (u Usage) IOAvailable() bool {
	return u.ReadOps != nil && u.WriteOps != nil
}
