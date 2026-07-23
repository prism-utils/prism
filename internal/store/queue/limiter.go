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

	sem     chan struct{}
	waiters atomic.Int64
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
			rejectTooMany(w)
			return
		}

		timer := time.NewTimer(l.Wait)
		defer timer.Stop()

		select {
		case l.sem <- struct{}{}:
			l.leaveWaitQueue()
			defer func() { <-l.sem }()
			next.ServeHTTP(w, r)
		case <-timer.C:
			l.leaveWaitQueue()
			rejectTooMany(w)
		case <-r.Context().Done():
			l.leaveWaitQueue()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			rejectTooMany(w)
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

func rejectTooMany(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	http.Error(w, tooManyBody, http.StatusTooManyRequests)
}
