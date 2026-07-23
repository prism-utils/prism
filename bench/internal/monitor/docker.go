package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

const defaultDockerInterval = 75 * time.Millisecond

// DockerSampler polls container cgroup stats via the Docker Engine API.
type DockerSampler struct {
	containerID string
	interval    time.Duration
	client      *http.Client
	statsURL    string
	fallback    bool

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	started time.Time
	stopped time.Time
	samples []dockerSample
	prev    dockerCumulative
	prevAt  time.Time
}

// NewDockerSampler returns a sampler for containerID using the Docker socket.
func NewDockerSampler(containerID string) (*DockerSampler, error) {
	client, base, err := dockerHTTPClient()
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(containerID)
	statsURL := base + "/containers/" + id + "/stats?stream=false&one-shot=true"
	return &DockerSampler{
		containerID: id,
		interval:    defaultDockerInterval,
		client:      client,
		statsURL:    statsURL,
		done:        make(chan struct{}),
	}, nil
}

// Start begins background polling until Stop or ctx cancellation.
func (d *DockerSampler) Start(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancel != nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.started = time.Now()
	d.samples = nil
	go d.loop(runCtx)
}

// Stop ends polling and returns aggregated Usage.
func (d *DockerSampler) Stop() Usage {
	d.mu.Lock()
	if d.cancel != nil {
		d.cancel()
		d.cancel = nil
	}
	d.stopped = time.Now()
	fallback := d.fallback
	d.mu.Unlock()
	<-d.done

	d.mu.Lock()
	defer d.mu.Unlock()
	if sample, err := d.fetchSample(context.Background()); err == nil {
		sample.at = d.stopped
		d.samples = append(d.samples, sample)
	}
	window := d.stopped.Sub(d.started).Seconds()
	u := aggregateDockerSamples(d.samples, window)
	if fallback {
		u.ReadOps = nil
		u.WriteOps = nil
	}
	return u
}

func (d *DockerSampler) loop(ctx context.Context) {
	defer close(d.done)
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	appendSample := func(at time.Time) {
		sample, err := d.fetchSample(ctx)
		if err != nil {
			if !d.fallback {
				if fb, fbErr := d.fetchFallback(ctx); fbErr == nil {
					d.mu.Lock()
					d.fallback = true
					fb.at = at
					d.samples = append(d.samples, fb)
					d.mu.Unlock()
				}
			}
			return
		}
		sample.at = at
		d.mu.Lock()
		d.samples = append(d.samples, sample)
		d.mu.Unlock()
	}
	appendSample(time.Now())

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			appendSample(now)
		}
	}
}

func (d *DockerSampler) fetchDiffSample(ctx context.Context) (dockerSample, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.statsURL, nil)
	if err != nil {
		return dockerSample{}, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return dockerSample{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return dockerSample{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return dockerSample{}, fmt.Errorf("monitor: docker stats status %d", resp.StatusCode)
	}
	cur, err := parseDockerStatsCumulative(body)
	if err != nil {
		return dockerSample{}, err
	}
	now := time.Now()
	d.mu.Lock()
	wall := now.Sub(d.prevAt)
	if d.prevAt.IsZero() {
		wall = 0
	}
	sample := diffDockerSample(d.prev, cur, wall, now)
	d.prev = cur
	d.prevAt = now
	d.mu.Unlock()
	return sample, nil
}

func (d *DockerSampler) fetchSample(ctx context.Context) (dockerSample, error) {
	return d.fetchDiffSample(ctx)
}

type dockerCLIFallback struct {
	CPUPerc  string `json:"CPUPerc"`
	MemUsage string `json:"MemUsage"`
}

func (d *DockerSampler) fetchFallback(ctx context.Context) (dockerSample, error) {
	//nolint:gosec // container id comes from compose ps, not user input.
	cmd := exec.CommandContext(ctx, "docker", "stats", "--no-stream", "--format", "{{json .}}", d.containerID)
	out, err := cmd.Output()
	if err != nil {
		return dockerSample{}, err
	}
	return parseDockerCLIStatsLine(string(out))
}

func parseDockerCLIStatsLine(out string) (dockerSample, error) {
	line := strings.TrimSpace(out)
	if line == "" {
		return dockerSample{}, fmt.Errorf("monitor: empty docker stats output")
	}
	var row dockerCLIFallback
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return dockerSample{}, fmt.Errorf("monitor: docker stats cli json: %w", err)
	}
	cpu := parseCPUPerc(row.CPUPerc)
	rss := parseMemUsageBytes(row.MemUsage)
	return dockerSample{cpuCores: cpu, rssBytes: rss}, nil
}

func parseCPUPerc(s string) float64 {
	s = strings.TrimSpace(strings.TrimSuffix(s, "%"))
	if s == "" {
		return 0
	}
	var v float64
	_, _ = fmt.Sscanf(s, "%f", &v)
	return v / 100.0
}

func parseMemUsageBytes(s string) uint64 {
	parts := strings.Split(s, "/")
	if len(parts) == 0 {
		return 0
	}
	return parseSizeToken(strings.TrimSpace(parts[0]))
}

func parseSizeToken(s string) uint64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	mult := uint64(1)
	switch {
	case strings.HasSuffix(s, "GiB"):
		mult = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GiB")
	case strings.HasSuffix(s, "MiB"):
		mult = 1024 * 1024
		s = strings.TrimSuffix(s, "MiB")
	case strings.HasSuffix(s, "KiB"):
		mult = 1024
		s = strings.TrimSuffix(s, "KiB")
	case strings.HasSuffix(s, "B"):
		s = strings.TrimSuffix(s, "B")
	}
	var v float64
	_, _ = fmt.Sscanf(strings.TrimSpace(s), "%f", &v)
	return uint64(v * float64(mult))
}

func dockerHTTPClient() (*http.Client, string, error) {
	host := os.Getenv("DOCKER_HOST")
	if host == "" {
		if runtime.GOOS == "windows" {
			host = "npipe:////./pipe/docker_engine"
		} else {
			host = "unix:///var/run/docker.sock"
		}
	}
	u, err := url.Parse(host)
	if err != nil {
		return nil, "", fmt.Errorf("monitor: DOCKER_HOST: %w", err)
	}
	switch u.Scheme {
	case "unix":
		socket := u.Path
		if socket == "" {
			socket = "/var/run/docker.sock"
		}
		tr := &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		}
		return &http.Client{Transport: tr}, "http://localhost/v1.45", nil
	case "tcp", "http", "https":
		base := u.String()
		if u.Scheme == "tcp" {
			base = "http://" + u.Host
		}
		return &http.Client{}, base + "/v1.45", nil
	default:
		return nil, "", fmt.Errorf("monitor: unsupported DOCKER_HOST scheme %q", u.Scheme)
	}
}

// ResolveComposeContainerID returns the running container id for a compose service.
func ResolveComposeContainerID(ctx context.Context, composeFile, service string) (string, error) {
	//nolint:gosec // compose path is repo-owned, service is a fixed harness constant.
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile, "ps", "-q", service)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("monitor: compose ps: %w", err)
	}
	id := strings.TrimSpace(string(out))
	if id == "" {
		return "", fmt.Errorf("monitor: no container for service %q", service)
	}
	if idx := strings.Index(id, "\n"); idx >= 0 {
		id = id[:idx]
	}
	return id, nil
}

var _ Sampler = (*ProcSampler)(nil)
var _ Sampler = (*DockerSampler)(nil)
