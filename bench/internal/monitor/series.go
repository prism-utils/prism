package monitor

import "time"

const (
	// PhaseIdle tags samples collected during the pre-workload quiet window.
	PhaseIdle = "idle"
	// PhaseIngest tags samples collected while loading metrics and logs.
	PhaseIngest = "ingest"
	// PhaseCount tags samples collected during full-table COUNT queries.
	PhaseCount = "count"
	// PhaseAggregation tags samples collected during GROUP BY aggregation queries.
	PhaseAggregation = "aggregation"
	// PhaseLogsLike tags samples collected during logs LIKE queries.
	PhaseLogsLike = "logs_like"
	// PhaseScanJSON tags samples collected during large-result JSON transport scans.
	PhaseScanJSON = "scan_json"
	// PhaseScanArrow tags samples collected during large-result Arrow transport scans.
	PhaseScanArrow = "scan_arrow"
)

// SamplePoint is one timestamped resource reading tagged with the active benchmark phase.
type SamplePoint struct {
	At       time.Time `json:"t"`
	Phase    string    `json:"phase"`
	CPUCores float64   `json:"cpu_cores"`
	RSSBytes uint64    `json:"rss_bytes"`
	ReadB    uint64    `json:"read_bytes,omitempty"`
	WriteB   uint64    `json:"write_bytes,omitempty"`
	ReadOps  uint64    `json:"read_ops,omitempty"`
	WriteOps uint64    `json:"write_ops,omitempty"`
}

// PhaseSpan marks a benchmark phase interval for time-based sample attribution.
type PhaseSpan struct {
	Name  string    `json:"name"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// AggregatePhase folds dense samples for one phase tag into Usage statistics.
func AggregatePhase(points []SamplePoint, phase string) Usage {
	var samples []procSample
	var windowSec float64
	ioOK := false
	for i, p := range points {
		if p.Phase != phase {
			continue
		}
		if i+1 < len(points) && points[i+1].Phase == phase {
			windowSec += points[i+1].At.Sub(p.At).Seconds()
		}
		samples = append(samples, procSample{
			cpuCores: p.CPUCores,
			rssBytes: p.RSSBytes,
			readB:    p.ReadB,
			writeB:   p.WriteB,
			readOps:  p.ReadOps,
			writeOps: p.WriteOps,
		})
		if p.ReadOps > 0 || p.WriteOps > 0 || p.ReadB > 0 || p.WriteB > 0 {
			ioOK = true
		}
	}
	if windowSec <= 0 && len(samples) > 1 {
		first, last := points[0].At, points[len(points)-1].At
		for _, p := range points {
			if p.Phase == phase {
				if p.At.Before(first) || first.IsZero() {
					first = p.At
				}
				if p.At.After(last) {
					last = p.At
				}
			}
		}
		windowSec = last.Sub(first).Seconds()
	}
	if windowSec <= 0 && len(samples) == 1 {
		windowSec = 0.05
	}
	return aggregateProcSamples(samples, windowSec, ioOK)
}

// AggregatePhaseSpan folds samples whose timestamps fall within the named phase span.
func AggregatePhaseSpan(points []SamplePoint, phase string, spans []PhaseSpan) Usage {
	var span *PhaseSpan
	for i := range spans {
		if spans[i].Name == phase {
			span = &spans[i]
			break
		}
	}
	if span == nil {
		return AggregatePhase(points, phase)
	}
	var filtered []SamplePoint
	for _, p := range points {
		if !p.At.Before(span.Start) && p.At.Before(span.End) {
			filtered = append(filtered, p)
		}
	}
	if len(filtered) == 0 {
		return Usage{}
	}
	windowSec := span.End.Sub(span.Start).Seconds()
	if windowSec <= 0 {
		windowSec = 0.05
	}
	var samples []procSample
	ioOK := false
	for _, p := range filtered {
		samples = append(samples, procSample{
			cpuCores: p.CPUCores,
			rssBytes: p.RSSBytes,
			readB:    p.ReadB,
			writeB:   p.WriteB,
			readOps:  p.ReadOps,
			writeOps: p.WriteOps,
		})
		if p.ReadOps > 0 || p.WriteOps > 0 || p.ReadB > 0 || p.WriteB > 0 {
			ioOK = true
		}
	}
	return aggregateProcSamples(samples, windowSec, ioOK)
}

// Downsample caps point count for chart/json output while preserving order.
func Downsample(points []SamplePoint, maxPoints int) []SamplePoint {
	if maxPoints <= 0 || len(points) <= maxPoints {
		out := make([]SamplePoint, len(points))
		copy(out, points)
		return out
	}
	step := float64(len(points)-1) / float64(maxPoints-1)
	out := make([]SamplePoint, 0, maxPoints)
	for i := 0; i < maxPoints; i++ {
		idx := int(float64(i) * step)
		if idx >= len(points) {
			idx = len(points) - 1
		}
		out = append(out, points[idx])
	}
	return out
}

// StitchStoreSeries merges store-binary samples with benchmark-process samples for chart timelines.
// storePhases lists phases attributed to the prism-store binary; remaining query phases use benchProc.
func StitchStoreSeries(storeBin, benchProc []SamplePoint, spans []PhaseSpan, storePhases []string) []SamplePoint {
	storeSet := make(map[string]struct{}, len(storePhases))
	for _, ph := range storePhases {
		storeSet[ph] = struct{}{}
	}
	out := make([]SamplePoint, 0, len(storeBin)+len(benchProc))
	for _, p := range storeBin {
		if phaseInAnySpan(p.At, storeSet, spans) {
			out = append(out, p)
		}
	}
	for _, p := range benchProc {
		if p.Phase == "" {
			continue
		}
		if _, ok := storeSet[p.Phase]; ok {
			continue
		}
		if phaseInAnySpan(p.At, map[string]struct{}{p.Phase: {}}, spans) {
			out = append(out, p)
		}
	}
	return out
}

func phaseInAnySpan(at time.Time, phases map[string]struct{}, spans []PhaseSpan) bool {
	for ph := range phases {
		if phaseInSpan(at, ph, spans) {
			return true
		}
	}
	return false
}

func phaseInSpan(at time.Time, name string, spans []PhaseSpan) bool {
	for _, sp := range spans {
		if sp.Name == name && !at.Before(sp.Start) && at.Before(sp.End) {
			return true
		}
	}
	return false
}
