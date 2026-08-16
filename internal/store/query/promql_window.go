package query

import (
	"net/http"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

func promQLOpenWindow(r *http.Request, lookback time.Duration) (time.Time, time.Time) {
	expr := r.Form.Get("query")
	if s := r.Form.Get("start"); s != "" {
		start, err1 := parseTimeParam(s, time.Time{})
		end, err2 := parseTimeParam(r.Form.Get("end"), time.Time{})
		if err1 == nil && err2 == nil && !start.IsZero() && !end.IsZero() {
			return expandPromQLWindow(start, end, expr, lookback)
		}
	}
	if t := r.Form.Get("time"); t != "" {
		ts, err := parseTimeParam(t, time.Time{})
		if err == nil && !ts.IsZero() {
			return expandPromQLWindow(ts, ts, expr, lookback)
		}
	}
	start, err1 := parseTimeParam(r.Form.Get("start"), defaultMinTime)
	end, err2 := parseTimeParam(r.Form.Get("end"), defaultMaxTime)
	if err1 == nil && err2 == nil {
		return expandPromQLWindow(start, end, expr, lookback)
	}
	return time.Time{}, time.Time{}
}

func expandPromQLWindow(start, end time.Time, expr string, lookback time.Duration) (time.Time, time.Time) {
	openStart := start
	if lookback > 0 {
		openStart = start.Add(-lookback)
	}
	if d := maxRangeSelector(expr); d > 0 && start.Add(-d).Before(openStart) {
		openStart = start.Add(-d)
	}
	openEnd := end.Add(time.Millisecond)
	if !openEnd.After(openStart) {
		openEnd = openStart.Add(time.Millisecond)
	}
	return openStart, openEnd
}

func maxRangeSelector(expr string) time.Duration {
	p := parser.NewParser(parser.Options{})
	e, err := p.ParseExpr(expr)
	if err != nil {
		return 0
	}
	var max time.Duration
	parser.Inspect(e, func(node parser.Node, _ []parser.Node) error {
		switch n := node.(type) {
		case *parser.MatrixSelector:
			if n.Range > max {
				max = n.Range
			}
		case *parser.SubqueryExpr:
			if n.Range > max {
				max = n.Range
			}
		}
		return nil
	})
	return max
}
