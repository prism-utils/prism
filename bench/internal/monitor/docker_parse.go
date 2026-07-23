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

type dockerCumulative struct {
	cpuTotalUsageNS uint64
	rssBytes        uint64
	readB           uint64
	writeB          uint64
	readOps         uint64
	writeOps        uint64
	blkioOK         bool
}

// parseDockerStatsCumulative extracts cumulative counters from one stats JSON body.
func parseDockerStatsCumulative(body []byte) (dockerCumulative, error) {
	var st dockerStatsPayload
	if err := json.Unmarshal(body, &st); err != nil {
		return dockerCumulative{}, fmt.Errorf("monitor: docker stats json: %w", err)
	}
	cur := dockerCumulative{
		cpuTotalUsageNS: st.CPUStats.CPUUsage.TotalUsage,
		rssBytes:        st.MemoryStats.Usage,
	}
	for _, e := range st.BlkioStats.IoServiceBytesRecursive {
		cur.blkioOK = true
		switch strings.ToLower(e.Op) {
		case "read":
			cur.readB += e.Value
		case "write":
			cur.writeB += e.Value
		}
	}
	for _, e := range st.BlkioStats.IoServicedRecursive {
		cur.blkioOK = true
		switch strings.ToLower(e.Op) {
		case "read":
			cur.readOps += e.Value
		case "write":
			cur.writeOps += e.Value
		}
	}
	return cur, nil
}

// diffDockerSample converts cumulative counter deltas into per-interval usage.
func diffDockerSample(prev, cur dockerCumulative, wall time.Duration, at time.Time) dockerSample {
	s := dockerSample{at: at, rssBytes: cur.rssBytes, blkioOK: cur.blkioOK}
	if wall > 0 && cur.cpuTotalUsageNS >= prev.cpuTotalUsageNS && prev.cpuTotalUsageNS > 0 {
		deltaNS := cur.cpuTotalUsageNS - prev.cpuTotalUsageNS
		s.cpuCores = float64(deltaNS) / float64(wall.Nanoseconds())
	}
	if cur.blkioOK {
		s.readB = deltaU64(cur.readB, prev.readB)
		s.writeB = deltaU64(cur.writeB, prev.writeB)
		s.readOps = deltaU64(cur.readOps, prev.readOps)
		s.writeOps = deltaU64(cur.writeOps, prev.writeOps)
	}
	return s
}

// parseDockerStatsSample extracts one CPU/mem/blkio sample from a stats JSON body
// using Docker-provided precpu_stats (legacy one-shot path).
func parseDockerStatsSample(body []byte) (cpuCores float64, rssBytes uint64, readB, writeB, readOps, writeOps uint64, blkioOK bool, err error) {
	cur, err := parseDockerStatsCumulative(body)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, false, err
	}
	var st dockerStatsPayload
	if err := json.Unmarshal(body, &st); err != nil {
		return 0, 0, 0, 0, 0, 0, false, err
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
	return cpuCores, cur.rssBytes, cur.readB, cur.writeB, cur.readOps, cur.writeOps, cur.blkioOK, nil
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
	var readB, writeB, readOps, writeOps uint64
	for _, s := range samples {
		readB += s.readB
		writeB += s.writeB
		readOps += s.readOps
		writeOps += s.writeOps
	}
	duration := windowSec
	if duration <= 0 && len(samples) > 1 {
		duration = samples[len(samples)-1].at.Sub(samples[0].at).Seconds()
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
