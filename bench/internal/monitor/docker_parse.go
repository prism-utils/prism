package monitor

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// dockerStatsPayload mirrors the subset of Docker Engine container stats JSON
// needed for CPU, memory, and blkio aggregation.
type dockerStatsPayload struct {
	CPUStats struct {
		CPUUsage struct {
			TotalUsage  uint64   `json:"total_usage"`
			PercpuUsage []uint64 `json:"percpu_usage"`
		} `json:"cpu_usage"`
		SystemCPUUsage uint64 `json:"system_cpu_usage"`
		OnlineCPUs     uint32 `json:"online_cpus"`
	} `json:"cpu_stats"`
	PreCPUStats struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
		SystemCPUUsage uint64 `json:"system_cpu_usage"`
	} `json:"precpu_stats"`
	MemoryStats struct {
		Usage uint64 `json:"usage"`
	} `json:"memory_stats"`
	BlkioStats struct {
		IoServiceBytesRecursive []blkioEntry `json:"io_service_bytes_recursive"`
		IoServicedRecursive     []blkioEntry `json:"io_serviced_recursive"`
	} `json:"blkio_stats"`
}

type blkioEntry struct {
	Op    string `json:"op"`
	Value uint64 `json:"value"`
}

type dockerSample struct {
	at       time.Time
	cpuCores float64
	rssBytes uint64
	readB    uint64
	writeB   uint64
	readOps  uint64
	writeOps uint64
	blkioOK  bool
}

// parseDockerStatsSample extracts one CPU/mem/blkio sample from a stats JSON body.
func parseDockerStatsSample(body []byte) (cpuCores float64, rssBytes uint64, readB, writeB, readOps, writeOps uint64, blkioOK bool, err error) {
	var st dockerStatsPayload
	if err := json.Unmarshal(body, &st); err != nil {
		return 0, 0, 0, 0, 0, 0, false, fmt.Errorf("monitor: docker stats json: %w", err)
	}
	cpuDelta := float64(st.CPUStats.CPUUsage.TotalUsage - st.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(st.CPUStats.SystemCPUUsage - st.PreCPUStats.SystemCPUUsage)
	if sysDelta > 0 {
		cpus := st.CPUStats.OnlineCPUs
		if cpus == 0 && len(st.CPUStats.CPUUsage.PercpuUsage) > 0 {
			cpus = uint32(len(st.CPUStats.CPUUsage.PercpuUsage)) //nolint:gosec // G115: CPU count is small.
		}
		if cpus == 0 {
			cpus = 1
		}
		cpuCores = (cpuDelta / sysDelta) * float64(cpus)
	}
	rssBytes = st.MemoryStats.Usage
	for _, e := range st.BlkioStats.IoServiceBytesRecursive {
		blkioOK = true
		switch strings.ToLower(e.Op) {
		case "read":
			readB += e.Value
		case "write":
			writeB += e.Value
		}
	}
	for _, e := range st.BlkioStats.IoServicedRecursive {
		blkioOK = true
		switch strings.ToLower(e.Op) {
		case "read":
			readOps += e.Value
		case "write":
			writeOps += e.Value
		}
	}
	if len(st.BlkioStats.IoServiceBytesRecursive) > 0 {
		blkioOK = true
	}
	return cpuCores, rssBytes, readB, writeB, readOps, writeOps, blkioOK, nil
}

// aggregateDockerSamples folds per-poll samples into Usage over the full window.
func aggregateDockerSamples(samples []dockerSample, windowSec float64) Usage {
	if len(samples) == 0 {
		return Usage{DurationSec: windowSec}
	}
	var meanSum float64
	peakCPU := 0.0
	peakRSS := uint64(0)
	for _, s := range samples {
		if s.cpuCores > peakCPU {
			peakCPU = s.cpuCores
		}
		meanSum += s.cpuCores
		if s.rssBytes > peakRSS {
			peakRSS = s.rssBytes
		}
	}
	start := samples[0]
	end := samples[len(samples)-1]
	readB := deltaU64(end.readB, start.readB)
	writeB := deltaU64(end.writeB, start.writeB)
	readOps := deltaU64(end.readOps, start.readOps)
	writeOps := deltaU64(end.writeOps, start.writeOps)
	duration := windowSec
	if duration <= 0 && len(samples) > 1 {
		duration = end.at.Sub(start.at).Seconds()
	}
	blkioOK := false
	for _, s := range samples {
		if s.blkioOK {
			blkioOK = true
			break
		}
	}
	u := Usage{
		CPUCoresMean: meanSum / float64(len(samples)),
		CPUCoresPeak: peakCPU,
		RSSPeakBytes: peakRSS,
		ReadBytes:    readB,
		WriteBytes:   writeB,
		DurationSec:  duration,
	}
	if blkioOK {
		ro, wo := readOps, writeOps
		u.ReadOps = &ro
		u.WriteOps = &wo
	}
	return u
}

func deltaU64(a, b uint64) uint64 {
	if a >= b {
		return a - b
	}
	return a
}
