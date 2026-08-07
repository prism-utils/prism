package queue

import (
	"net/http"
	"sync/atomic"
	"time"
)

const tooManyBody = "too many concurrent queries"

// LimiterConfig holds in-flight queue parameters for POST /sql.
type LimiterConfig struct {
	Enabled     bool
	MaxInFlight int
	MaxQueue    int
	Wait        time.Duration
}

// Limiter bounds concurrent /sql handler executions with a bounded wait queue.
type Limiter struct {
	Enabled     bool
	MaxInFlight int
	MaxQueue    int
	Wait        time.Duration

	sem           chan struct{}
	waiters       atomic.Int64
	rejectedTotal atomic.Int64
}

// Snapshot is a point-in-time view of limiter caps and occupancy for operators.
// Counters are cumulative since process start; gauges are instantaneous and may
// already be stale by the time a caller reads them.
type Snapshot struct {
	Enabled       bool  `json:"enabled"`
	MaxInFlight   int   `json:"maxInFlight"`
	MaxQueue      int   `json:"maxQueue"`
	TimeoutMs     int64 `json:"timeoutMs"`
	InFlight      int   `json:"inFlight"`
	Waiting       int64 `json:"waiting"`
	RejectedTotal int64 `json:"rejectedTotal"`
}

// Snapshot reads the limiter state with atomic loads only; a nil limiter reports
// a disabled queue so callers without one need no special case.
func (l *Limiter) Snapshot() Snapshot {
	if l == nil {
		return Snapshot{}
	}
	return Snapshot{
		Enabled:       l.Enabled,
		MaxInFlight:   l.MaxInFlight,
		MaxQueue:      l.MaxQueue,
		TimeoutMs:     l.Wait.Milliseconds(),
		InFlight:      len(l.sem),
		Waiting:       l.waiters.Load(),
		RejectedTotal: l.rejectedTotal.Load(),
	}
}

// NewLimiter builds a limiter with a buffered-channel semaphore sized to MaxInFlight.
func NewLimiter(cfg LimiterConfig) *Limiter {
	maxInFlight := cfg.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = 1
	}
	return &Limiter{
		Enabled:     cfg.Enabled,
		MaxInFlight: maxInFlight,
		MaxQueue:    cfg.MaxQueue,
		Wait:        cfg.Wait,
		sem:         make(chan struct{}, maxInFlight),
	}
}

// Middleware gates next when Enabled; otherwise it is a transparent passthrough.
func Middleware(l *Limiter, next http.Handler) http.Handler {
	if l == nil || !l.Enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case l.sem <- struct{}{}:
			defer func() { <-l.sem }()
			next.ServeHTTP(w, r)
			return
		default:
		}

		if !l.enterWaitQueue() {
			l.rejectTooMany(w)
			return
		}
		// Guarantee the waiter count is decremented exactly once even if a later
		// step panics; the explicit calls free the waiter slot before serving.
		releaseWaiter := releaseOnce(l)
		defer releaseWaiter()

		timer := time.NewTimer(l.Wait)
		defer timer.Stop()

		select {
		case l.sem <- struct{}{}:
			releaseWaiter()
			defer func() { <-l.sem }()
			next.ServeHTTP(w, r)
		case <-timer.C:
			releaseWaiter()
			l.rejectTooMany(w)
		case <-r.Context().Done():
			releaseWaiter()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			l.rejectTooMany(w)
		}
	})
}

func (l *Limiter) enterWaitQueue() bool {
	for {
		cur := l.waiters.Load()
		if int64(l.MaxQueue) <= cur {
			return false
		}
		if l.waiters.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (l *Limiter) leaveWaitQueue() {
	l.waiters.Add(-1)
}

// releaseOnce returns an idempotent decrement of the waiter count so both the
// explicit call and the deferred safety net collapse to a single Add(-1).
func releaseOnce(l *Limiter) func() {
	released := false
	return func() {
		if released {
			return
		}
		released = true
		l.leaveWaitQueue()
	}
}

// rejectTooMany sheds one request and counts it, so every shed path — full wait
// queue, expired wait, cancelled client — is visible in a single total.
func (l *Limiter) rejectTooMany(w http.ResponseWriter) {
	l.rejectedTotal.Add(1)
	w.Header().Set("Retry-After", "1")
	http.Error(w, tooManyBody, http.StatusTooManyRequests)
}
