package notify

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Sender delivers one assembled webhook payload. The webhook client is the
// production implementation; tests inject a recorder.
type Sender interface {
	Send(ctx context.Context, payload WebhookPayload) error
}

// Options configures the dispatcher's Alertmanager-style route knobs.
type Options struct {
	Receiver       string
	ExternalURL    string
	GroupBy        []string
	GroupWait      time.Duration
	GroupInterval  time.Duration
	RepeatInterval time.Duration
	ResolveTimeout time.Duration
}

// Dispatcher groups incoming alerts by their group_by label tuple and flushes
// each group as a single webhook, honoring group_wait (first notification),
// group_interval (spacing after a change), and repeat_interval (re-send of an
// unchanged firing group). It is safe for concurrent Ingest and Tick.
type Dispatcher struct {
	opts   Options
	sender Sender
	logger *slog.Logger

	mu     sync.Mutex
	groups map[string]*aggrGroup
}

// aggrGroup is the mutable state for one group_by tuple.
type aggrGroup struct {
	groupLabels map[string]string
	alerts      map[string]Alert // keyed by alert fingerprint
	lastFlush   time.Time
	flushAt     time.Time // zero = no flush scheduled
}

// NewDispatcher builds a dispatcher. A nil logger is replaced with a discard
// logger so callers never nil-check.
func NewDispatcher(opts Options, sender Sender, logger *slog.Logger) *Dispatcher {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	return &Dispatcher{
		opts:   opts,
		sender: sender,
		logger: logger,
		groups: map[string]*aggrGroup{},
	}
}

// Ingest folds a batch of alerts into their groups and (re)schedules flushes.
// A brand-new group is scheduled group_wait ahead; a change to an already-sent
// group is scheduled group_interval after its last flush. Scheduling only ever
// moves a flush earlier, so batches coalesce instead of delaying notification.
func (d *Dispatcher) Ingest(now time.Time, alerts []Alert) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, a := range alerts {
		gl := groupLabelsFor(a.Labels, d.opts.GroupBy)
		key := groupKeyFor(gl)
		g := d.groups[key]
		if g == nil {
			g = &aggrGroup{groupLabels: gl, alerts: map[string]Alert{}}
			d.groups[key] = g
		}
		fp := fingerprint(a.Labels)
		prev, existed := g.alerts[fp]
		// A change is a newly-seen series or a firing↔resolved transition. A
		// repeated identical alert (the ruler resends every eval) is not a
		// change, so it must not pull a group_interval notification forward —
		// only repeat_interval re-sends an unchanged group.
		changed := !existed || prev.Resolved != a.Resolved
		g.alerts[fp] = a
		if !changed {
			continue
		}

		var candidate time.Time
		if g.lastFlush.IsZero() {
			candidate = now.Add(d.opts.GroupWait)
		} else {
			candidate = laterOf(now, g.lastFlush.Add(d.opts.GroupInterval))
		}
		if g.flushAt.IsZero() || candidate.Before(g.flushAt) {
			g.flushAt = candidate
		}
	}
}

// Tick flushes every group whose scheduled time has arrived and sends the
// resulting payloads. Sends happen outside the lock so a slow notifier never
// blocks Ingest. A send failure is logged and dropped; the next change or
// repeat re-notifies (fail-open, bounded).
func (d *Dispatcher) Tick(ctx context.Context, now time.Time) {
	for _, p := range d.collectDue(now) {
		if err := d.sender.Send(ctx, p); err != nil {
			d.logger.Error("notify send failed", "group_key", p.GroupKey, "err", err)
		}
	}
}

// collectDue advances scheduling state and returns the payloads to send, in a
// deterministic (sorted-key) order.
func (d *Dispatcher) collectDue(now time.Time) []WebhookPayload {
	d.mu.Lock()
	defer d.mu.Unlock()

	keys := make([]string, 0, len(d.groups))
	for k, g := range d.groups {
		if !g.flushAt.IsZero() && !now.Before(g.flushAt) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	payloads := make([]WebhookPayload, 0, len(keys))
	for _, k := range keys {
		g := d.groups[k]
		alerts := make([]Alert, 0, len(g.alerts))
		fps := make([]string, 0, len(g.alerts))
		for fp := range g.alerts {
			fps = append(fps, fp)
		}
		sort.Strings(fps)
		for _, fp := range fps {
			alerts = append(alerts, g.alerts[fp])
		}
		if len(alerts) == 0 {
			delete(d.groups, k)
			continue
		}

		payloads = append(payloads, buildPayload(
			d.opts.Receiver, d.opts.ExternalURL, g.groupLabels, alerts, now, d.opts.ResolveTimeout,
		))

		g.lastFlush = now
		for fp, a := range g.alerts {
			if a.Resolved {
				delete(g.alerts, fp)
			}
		}
		if len(g.alerts) > 0 {
			g.flushAt = now.Add(d.opts.RepeatInterval)
		} else {
			delete(d.groups, k)
		}
	}
	return payloads
}

// Run drives Tick on a fixed cadence until ctx is cancelled. The cadence is the
// dispatch resolution, not the notification cadence (which the route knobs set).
func (d *Dispatcher) Run(ctx context.Context, clock func() time.Time, resolution time.Duration) {
	if resolution <= 0 {
		resolution = time.Second
	}
	t := time.NewTicker(resolution)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.Tick(ctx, clock())
		}
	}
}

func laterOf(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// discard is an io.Writer that drops everything; used for the fallback logger.
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
