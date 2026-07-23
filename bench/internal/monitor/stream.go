package monitor

import (
	"context"
	"sync"
	"time"
)

const (
	defaultProcStreamInterval   = 35 * time.Millisecond
	defaultDockerStreamInterval = 75 * time.Millisecond
)

// StreamSampler polls a target continuously and records phase-tagged samples.
type StreamSampler struct {
	interval time.Duration
	poll     func(context.Context) (SamplePoint, error)

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	phase   string
	started time.Time
	points  []SamplePoint
}

// NewProcStreamSampler polls pid and descendants at ~20–50 ms.
func NewProcStreamSampler(pid int) *StreamSampler {
	ps := NewProcSampler(pid)
	ps.interval = defaultProcStreamInterval
	return &StreamSampler{
		interval: defaultProcStreamInterval,
		poll: func(ctx context.Context) (SamplePoint, error) {
			return ps.pollOnce(ctx)
		},
		done: make(chan struct{}),
	}
}

// NewDockerStreamSampler polls a container at ~50–100 ms using cumulative counter diffs.
func NewDockerStreamSampler(containerID string) (*StreamSampler, error) {
	ds, err := NewDockerSampler(containerID)
	if err != nil {
		return nil, err
	}
	ds.interval = defaultDockerStreamInterval
	return &StreamSampler{
		interval: defaultDockerStreamInterval,
		poll: func(ctx context.Context) (SamplePoint, error) {
			return ds.pollOnce(ctx)
		},
		done: make(chan struct{}),
	}, nil
}

// SetPhase tags subsequent samples with the benchmark phase name.
func (s *StreamSampler) SetPhase(phase string) {
	s.mu.Lock()
	s.phase = phase
	s.mu.Unlock()
}

// ForceSample polls immediately so short phases still capture at least one reading.
func (s *StreamSampler) ForceSample(ctx context.Context) {
	s.appendPoll(ctx)
}

// Start begins background polling until Stop or ctx cancellation.
func (s *StreamSampler) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.started = time.Now()
	s.points = nil
	go s.loop(runCtx)
}

func (s *StreamSampler) appendPoll(ctx context.Context) {
	pt, err := s.poll(ctx)
	if err != nil {
		return
	}
	s.mu.Lock()
	pt.Phase = s.phase
	if pt.At.IsZero() {
		pt.At = time.Now()
	}
	s.points = append(s.points, pt)
	s.mu.Unlock()
}

// Stop ends polling and returns collected samples.
func (s *StreamSampler) Stop() []SamplePoint {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	s.mu.Unlock()
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SamplePoint, len(s.points))
	copy(out, s.points)
	return out
}

func (s *StreamSampler) loop(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	appendPoll := func() {
		s.appendPoll(ctx)
	}
	appendPoll()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			appendPoll()
		}
	}
}

// pollOnce reads one proc sample without mutating ProcSampler state used by Stop().
func (p *ProcSampler) pollOnce(ctx context.Context) (SamplePoint, error) {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	p.appendSampleLocked(now)
	if len(p.samples) == 0 {
		return SamplePoint{At: now}, nil
	}
	last := p.samples[len(p.samples)-1]
	p.samples = p.samples[:0]
	return SamplePoint{
		At:       now,
		CPUCores: last.cpuCores,
		RSSBytes: last.rssBytes,
		ReadB:    last.readB,
		WriteB:   last.writeB,
		ReadOps:  last.readOps,
		WriteOps: last.writeOps,
	}, nil
}

func (d *DockerSampler) pollOnce(ctx context.Context) (SamplePoint, error) {
	sample, err := d.fetchDiffSample(ctx)
	if err != nil {
		return SamplePoint{}, err
	}
	return SamplePoint{
		At:       sample.at,
		CPUCores: sample.cpuCores,
		RSSBytes: sample.rssBytes,
		ReadB:    sample.readB,
		WriteB:   sample.writeB,
		ReadOps:  sample.readOps,
		WriteOps: sample.writeOps,
	}, nil
}
