// Package ruler loads Prometheus alerting-rule YAML, evaluates each rule against
// prism-store's PromQL API at the current time, and drives the
// pending→firing→resolved state machine (honoring `for` and `keep_firing_for`)
// with `$value`/`$labels` annotation templating. It forwards firing and
// resolved alerts to a sink (the notifier dispatcher).
//
// It deliberately reuses only the canonical, dependency-light Prometheus
// packages — rule parsing (model/rulefmt), expression validation
// (promql/parser), and template expansion (template) — instead of the full
// rules.Manager. The manager transitively pulls the Alertmanager notifier and
// its cloud service-discovery config (AWS/Azure/GCP SDKs), which would bloat
// this stateless ruler's binary and attack surface for no benefit: prism-alert
// keeps no TSDB and talks to exactly one notifier over one webhook.
package ruler

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/template"

	"github.com/elk-utilities/prism/internal/alert/notify"
)

// QueryFunc evaluates a PromQL instant query at time t. The prism-store PromQL
// client is the production implementation.
type QueryFunc func(ctx context.Context, q string, t time.Time) (promql.Vector, error)

// AlertSink receives the alerts produced by one evaluation cycle, stamped with
// the evaluation clock.
type AlertSink func(now time.Time, alerts []notify.Alert)

// Config is the ruler's evaluation configuration.
type Config struct {
	RulesDir           string
	EvaluationInterval time.Duration
	ExternalURL        string
	// ResendDelay is how often an unchanged firing alert is re-sent; 0 falls
	// back to EvaluationInterval.
	ResendDelay time.Duration
}

// Ruler evaluates alerting rules on a fixed cadence.
type Ruler struct {
	rules          []*alertRule
	query          QueryFunc
	sink           AlertSink
	clock          func() time.Time
	interval       time.Duration
	resendDelay    time.Duration
	externalURL    *url.URL
	externalURLStr string
	logger         *slog.Logger
	files          []string
}

// alertRule is one compiled alerting rule plus its active-alert state.
type alertRule struct {
	name          string
	expr          string
	forDur        time.Duration
	keepFiringFor time.Duration
	labels        map[string]string
	annotations   map[string]string
	active        map[uint64]*activeAlert
}

// activeAlert tracks one label set the rule's expression currently (or recently)
// produced.
type activeAlert struct {
	labels          labels.Labels
	annotations     labels.Labels
	value           float64
	activeAt        time.Time
	firedAt         time.Time
	resolvedAt      time.Time
	lastSentAt      time.Time
	keepFiringSince time.Time
	firing          bool
}

// New compiles the rule files and builds a Ruler. clock/logger are injectable
// for tests (nil → time.Now / discard).
func New(cfg Config, query QueryFunc, sink AlertSink, logger *slog.Logger, clock func() time.Time) (*Ruler, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	if clock == nil {
		clock = time.Now
	}
	files, err := ruleFiles(cfg.RulesDir)
	if err != nil {
		return nil, err
	}
	extURL, err := url.Parse(cfg.ExternalURL)
	if err != nil {
		return nil, fmt.Errorf("parse external url: %w", err)
	}
	resend := cfg.ResendDelay
	if resend <= 0 {
		resend = cfg.EvaluationInterval
	}

	rules, err := compileRules(files, logger)
	if err != nil {
		return nil, err
	}

	return &Ruler{
		rules:          rules,
		query:          query,
		sink:           sink,
		clock:          clock,
		interval:       cfg.EvaluationInterval,
		resendDelay:    resend,
		externalURL:    extURL,
		externalURLStr: cfg.ExternalURL,
		logger:         logger,
		files:          files,
	}, nil
}

// Files returns the loaded rule files (sorted).
func (r *Ruler) Files() []string { return r.files }

// Run evaluates all rules immediately and then every EvaluationInterval until
// ctx is cancelled. It blocks; run it on its own goroutine.
func (r *Ruler) Run(ctx context.Context) error {
	r.evalAll(ctx)
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.evalAll(ctx)
		}
	}
}

func (r *Ruler) evalAll(ctx context.Context) {
	now := r.clock()
	var batch []notify.Alert
	for _, rule := range r.rules {
		alerts, err := r.evalRule(ctx, rule, now)
		if err != nil {
			// Fail-open: a query failure leaves this rule's alert state
			// untouched (no spurious resolve) and is logged, not fatal.
			r.logger.Warn("rule evaluation failed", "alert", rule.name, "err", err)
			continue
		}
		batch = append(batch, alerts...)
	}
	if len(batch) > 0 {
		r.sink(now, batch)
	}
}

// evalRule advances one rule's state machine for evaluation time ts and returns
// the alerts that need sending this cycle.
func (r *Ruler) evalRule(ctx context.Context, rule *alertRule, ts time.Time) ([]notify.Alert, error) {
	vec, err := r.query(ctx, rule.expr, ts)
	if err != nil {
		return nil, err
	}

	resultFPs := make(map[uint64]struct{}, len(vec))
	for _, smpl := range vec {
		lbs, annos := r.expand(ctx, rule, smpl, ts)
		h := lbs.Hash()
		resultFPs[h] = struct{}{}

		a := rule.active[h]
		if a == nil {
			a = &activeAlert{activeAt: ts}
			rule.active[h] = a
		}
		a.labels = lbs
		a.annotations = annos
		a.value = smpl.F
		a.resolvedAt = time.Time{}
		a.keepFiringSince = time.Time{}
		if ts.Sub(a.activeAt) >= rule.forDur {
			if !a.firing {
				a.firedAt = ts
			}
			a.firing = true
		} else {
			a.firing = false // still pending
		}
	}

	// Series that stopped matching either keep firing (keep_firing_for) or resolve.
	for h, a := range rule.active {
		if _, ok := resultFPs[h]; ok {
			continue
		}
		if rule.keepFiringFor > 0 && a.firing {
			if a.keepFiringSince.IsZero() {
				a.keepFiringSince = ts
			}
			if ts.Sub(a.keepFiringSince) < rule.keepFiringFor {
				continue
			}
		}
		if a.firedAt.IsZero() {
			// Never fired (cleared while pending): drop silently, no notification.
			delete(rule.active, h)
			continue
		}
		if a.resolvedAt.IsZero() {
			a.resolvedAt = ts
		}
		a.firing = false
	}

	var toSend []notify.Alert
	for h, a := range rule.active {
		resolved := !a.resolvedAt.IsZero()
		switch {
		case resolved:
			if !a.resolvedAt.After(a.lastSentAt) {
				break
			}
			a.lastSentAt = ts
			toSend = append(toSend, r.notifyAlert(rule, a, true))
		case a.firing:
			if !a.lastSentAt.Add(r.resendDelay).Before(ts) {
				break
			}
			a.lastSentAt = ts
			toSend = append(toSend, r.notifyAlert(rule, a, false))
		}
		// Prune a resolved alert once its resolution has been delivered.
		if resolved && !a.lastSentAt.Before(a.resolvedAt) {
			delete(rule.active, h)
		}
	}
	return toSend, nil
}

func (r *Ruler) notifyAlert(rule *alertRule, a *activeAlert, resolved bool) notify.Alert {
	n := notify.Alert{
		Labels:       a.labels.Map(),
		Annotations:  a.annotations.Map(),
		StartsAt:     a.firedAt,
		GeneratorURL: r.generatorURL(rule.expr),
	}
	if resolved {
		n.Resolved = true
		n.ResolvedAt = a.resolvedAt
	}
	return n
}

// generatorURL builds an expression permalink shaped like Prometheus' own so a
// receiver can render a "Source" link back to a UI when ExternalURL is set.
func (r *Ruler) generatorURL(expr string) string {
	if r.externalURLStr == "" {
		return ""
	}
	return r.externalURLStr + "/graph?g0.expr=" + url.QueryEscape(expr) + "&g0.tab=1"
}

// expand produces one alert's final label set and annotations for a result
// sample, applying rule labels (templated), forcing alertname, and expanding
// $value/$labels in annotations — the same shape Prometheus' alerting rules use.
func (r *Ruler) expand(ctx context.Context, rule *alertRule, smpl promql.Sample, ts time.Time) (labels.Labels, labels.Labels) {
	metricLabels := smpl.Metric.Map()
	tmplData := template.AlertTemplateData(metricLabels, map[string]string{}, r.externalURLStr, smpl)
	defs := []string{
		"{{$labels := .Labels}}",
		"{{$externalLabels := .ExternalLabels}}",
		"{{$externalURL := .ExternalURL}}",
		"{{$value := .Value}}",
	}
	expand := func(text string) string {
		tmpl := template.NewTemplateExpander(
			ctx,
			strings.Join(append(defs, text), ""),
			"__alert_"+rule.name,
			tmplData,
			model.Time(ts.UnixMilli()),
			template.QueryFunc(func(c context.Context, q string, t time.Time) (promql.Vector, error) {
				return r.query(c, q, t)
			}),
			r.externalURL,
			nil,
		)
		out, err := tmpl.Expand()
		if err != nil {
			r.logger.Warn("expand alert template failed", "alert", rule.name, "err", err)
			return fmt.Sprintf("<error expanding template: %s>", err)
		}
		return out
	}

	lb := labels.NewBuilder(smpl.Metric)
	lb.Del(labels.MetricName)
	for name, value := range rule.labels {
		lb.Set(name, expand(value))
	}
	lb.Set(labels.AlertName, rule.name)

	ab := labels.NewBuilder(labels.EmptyLabels())
	for name, value := range rule.annotations {
		ab.Set(name, expand(value))
	}
	return lb.Labels(), ab.Labels()
}

// compileRules parses every file into alerting rules, validating each
// expression. Recording rules are skipped (prism-alert records nothing).
func compileRules(files []string, logger *slog.Logger) ([]*alertRule, error) {
	p := parser.NewParser(parser.Options{})
	var out []*alertRule
	for _, f := range files {
		groups, errs := rulefmt.ParseFile(f, false, model.UTF8Validation, p, logger)
		if len(errs) > 0 {
			return nil, fmt.Errorf("parse rule file %s: %w", f, errs[0])
		}
		for _, g := range groups.Groups {
			for _, rl := range g.Rules {
				if rl.Alert == "" {
					continue // recording rule
				}
				// rulefmt.ParseFile already validated every expression with the
				// parser above, so a malformed expr surfaces as a parse error.
				out = append(out, &alertRule{
					name:          rl.Alert,
					expr:          rl.Expr,
					forDur:        time.Duration(rl.For),
					keepFiringFor: time.Duration(rl.KeepFiringFor),
					labels:        rl.Labels,
					annotations:   rl.Annotations,
					active:        map[uint64]*activeAlert{},
				})
			}
		}
	}
	return out, nil
}

// ruleFiles returns the sorted *.yml / *.yaml files in dir. A missing directory
// yields no files so a not-yet-mounted ConfigMap does not crash startup.
func ruleFiles(dir string) ([]string, error) {
	if dir == "" {
		return nil, nil
	}
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat rules dir: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("rules dir %q is not a directory", dir)
	}
	var files []string
	for _, ext := range []string{"*.yml", "*.yaml"} {
		matches, err := filepath.Glob(filepath.Join(dir, ext))
		if err != nil {
			return nil, fmt.Errorf("glob rules dir: %w", err)
		}
		files = append(files, matches...)
	}
	sort.Strings(files)
	return files, nil
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
