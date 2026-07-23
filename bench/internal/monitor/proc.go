package monitor

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/process"
)

const defaultProcInterval = 75 * time.Millisecond

// ProcSampler polls a process tree for CPU, RSS, and (on Linux) I/O counters.
type ProcSampler struct {
	pid      int32
	interval time.Duration
	ioOK     bool

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	started time.Time
	stopped time.Time
	samples []procSample
	prevCPU map[int32]cpuSnap
	prevIO  map[int32]ioSnap
	prevAt  time.Time
}

type procSample struct {
	cpuCores float64
	rssBytes uint64
	readB    uint64
	writeB   uint64
	readOps  uint64
	writeOps uint64
}

// NewProcSampler returns a sampler for pid and its descendants.
func NewProcSampler(pid int) *ProcSampler {
	return &ProcSampler{
		pid:      int32(pid), //nolint:gosec // G115: benchmark PIDs fit int32.
		interval: defaultProcInterval,
		ioOK:     runtime.GOOS == "linux",
		done:     make(chan struct{}),
	}
}

// Start begins background polling until Stop or ctx cancellation.
func (p *ProcSampler) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.started = time.Now()
	p.samples = nil
	go p.loop(runCtx)
}

// Stop ends polling and returns aggregated Usage.
func (p *ProcSampler) Stop() Usage {
	p.mu.Lock()
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	p.stopped = time.Now()
	p.mu.Unlock()
	<-p.done

	p.mu.Lock()
	defer p.mu.Unlock()
	p.appendSampleLocked(time.Now())
	window := p.stopped.Sub(p.started).Seconds()
	return aggregateProcSamples(p.samples, window, p.ioOK)
}

func (p *ProcSampler) appendSampleLocked(now time.Time) {
	pids, err := collectPIDs(p.pid)
	if err != nil || len(pids) == 0 {
		return
	}
	prevCPU := p.prevCPU
	prevIO := p.prevIO
	prevAt := p.prevAt

	cpuCores, cpuSnap := sumCPUCores(pids, prevCPU, prevAt, now)
	rss, _ := sumRSS(pids)
	readB, writeB, readOps, writeOps, ioSnap, ioAvail := sumIO(pids, prevIO, p.ioOK)
	if ioAvail {
		p.ioOK = true
	}
	p.prevCPU = cpuSnap
	p.prevIO = ioSnap
	p.prevAt = now

	p.samples = append(p.samples, procSample{
		cpuCores: cpuCores,
		rssBytes: rss,
		readB:    readB,
		writeB:   writeB,
		readOps:  readOps,
		writeOps: writeOps,
	})
}

func (p *ProcSampler) loop(ctx context.Context) {
	defer close(p.done)
	p.mu.Lock()
	p.prevCPU = nil
	p.prevIO = nil
	p.prevAt = time.Time{}
	p.appendSampleLocked(time.Now())
	p.mu.Unlock()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			p.mu.Lock()
			p.appendSampleLocked(now)
			p.mu.Unlock()
		}
	}
}

type cpuSnap struct {
	user, system float64
}

type ioSnap struct {
	readB, writeB, readOps, writeOps uint64
}

func collectPIDs(root int32) ([]int32, error) {
	proc, err := process.NewProcess(root)
	if err != nil {
		return nil, err
	}
	out := []int32{root}
	children, _ := proc.Children()
	for _, c := range children {
		out = append(out, c.Pid)
		grand, _ := collectPIDs(c.Pid)
		out = append(out, grand...)
	}
	return out, nil
}

func sumCPUCores(pids []int32, prev map[int32]cpuSnap, prevAt, now time.Time) (float64, map[int32]cpuSnap) {
	wall := now.Sub(prevAt).Seconds()
	if wall <= 0 || prev == nil {
		snap := snapshotCPU(pids)
		return 0, snap
	}
	cur := snapshotCPU(pids)
	var cores float64
	for _, pid := range pids {
		a, okA := prev[pid]
		b, okB := cur[pid]
		if !okA || !okB {
			continue
		}
		delta := (b.user + b.system) - (a.user + a.system)
		if delta > 0 {
			cores += delta / wall
		}
	}
	return cores, cur
}

func snapshotCPU(pids []int32) map[int32]cpuSnap {
	out := make(map[int32]cpuSnap, len(pids))
	for _, pid := range pids {
		proc, err := process.NewProcess(pid)
		if err != nil {
			continue
		}
		times, err := proc.Times()
		if err != nil {
			continue
		}
		out[pid] = cpuSnap{user: times.User, system: times.System}
	}
	return out
}

func sumRSS(pids []int32) (uint64, error) {
	var total uint64
	for _, pid := range pids {
		proc, err := process.NewProcess(pid)
		if err != nil {
			continue
		}
		mem, err := proc.MemoryInfo()
		if err != nil {
			continue
		}
		total += mem.RSS
	}
	return total, nil
}

func sumIO(pids []int32, prev map[int32]ioSnap, enabled bool) (readB, writeB, readOps, writeOps uint64, cur map[int32]ioSnap, ioAvail bool) {
	if !enabled {
		return 0, 0, 0, 0, prev, false
	}
	cur = make(map[int32]ioSnap, len(pids))
	ioAvail = true
	for _, pid := range pids {
		proc, err := process.NewProcess(pid)
		if err != nil {
			ioAvail = false
			continue
		}
		io, err := proc.IOCounters()
		if err != nil {
			ioAvail = false
			continue
		}
		cur[pid] = ioSnap{
			readB:    io.ReadBytes,
			writeB:   io.WriteBytes,
			readOps:  io.ReadCount,
			writeOps: io.WriteCount,
		}
	}
	if prev == nil || !ioAvail {
		return 0, 0, 0, 0, cur, ioAvail
	}
	for _, pid := range pids {
		a, okA := prev[pid]
		b, okB := cur[pid]
		if !okA || !okB {
			continue
		}
		readB += deltaU64(b.readB, a.readB)
		writeB += deltaU64(b.writeB, a.writeB)
		readOps += deltaU64(b.readOps, a.readOps)
		writeOps += deltaU64(b.writeOps, a.writeOps)
	}
	return readB, writeB, readOps, writeOps, cur, ioAvail
}

func aggregateProcSamples(samples []procSample, windowSec float64, ioOK bool) Usage {
	if len(samples) == 0 {
		return Usage{DurationSec: windowSec}
	}
	var meanSum float64
	peakCPU := 0.0
	peakRSS := uint64(0)
	var readB, writeB, readOps, writeOps uint64
	for _, s := range samples {
		if s.cpuCores > peakCPU {
			peakCPU = s.cpuCores
		}
		meanSum += s.cpuCores
		if s.rssBytes > peakRSS {
			peakRSS = s.rssBytes
		}
		readB += s.readB
		writeB += s.writeB
		readOps += s.readOps
		writeOps += s.writeOps
	}
	u := Usage{
		CPUCoresMean: meanSum / float64(len(samples)),
		CPUCoresPeak: peakCPU,
		RSSPeakBytes: peakRSS,
		ReadBytes:    readB,
		WriteBytes:   writeB,
		DurationSec:  windowSec,
	}
	if ioOK && (readOps > 0 || writeOps > 0 || readB > 0 || writeB > 0) {
		ro, wo := readOps, writeOps
		u.ReadOps = &ro
		u.WriteOps = &wo
	}
	return u
}
