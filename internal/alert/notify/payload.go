// Package notify turns ruler alerts into Alertmanager-compatible notifications.
// It owns the grouping/dispatch state machine (group_by, group_wait,
// group_interval, repeat_interval, resolve_timeout) and emits the exact
// Alertmanager v4 webhook payload the prism notifier already consumes.
package notify

import (
	"sort"
	"strings"
	"time"

	"github.com/prometheus/common/model"
)

const (
	statusFiring   = "firing"
	statusResolved = "resolved"
	// webhookVersion is the Alertmanager webhook schema version the notifier
	// contract pins ("4").
	webhookVersion = "4"
)

// Alert is the transport-neutral view of one alert instance the ruler hands to
// the dispatcher. It carries no Prometheus types so the notify layer stays
// decoupled and independently testable.
type Alert struct {
	// Resolved is true when the rule stopped producing this series.
	Resolved     bool
	Labels       map[string]string
	Annotations  map[string]string
	StartsAt     time.Time
	ResolvedAt   time.Time
	GeneratorURL string
}

// WebhookAlert is one alert inside the Alertmanager v4 webhook body.
type WebhookAlert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     string            `json:"startsAt"`
	EndsAt       string            `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// WebhookPayload is the Alertmanager v4 webhook body. Field names and casing
// are frozen by the notifier contract.
type WebhookPayload struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	Status            string            `json:"status"`
	Receiver          string            `json:"receiver"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []WebhookAlert    `json:"alerts"`
}

// fingerprint is the stable Alertmanager identifier for an alert's label set:
// the 64-bit label fingerprint rendered as 16 hex digits. It lets a receiver
// dedupe firing/resolved pairs for the same series.
func fingerprint(lbls map[string]string) string {
	ls := make(model.LabelSet, len(lbls))
	for k, v := range lbls {
		ls[model.LabelName(k)] = model.LabelValue(v)
	}
	return ls.Fingerprint().String()
}

// groupKeyFor renders a deterministic, Alertmanager-shaped opaque key from the
// group's label tuple: {k1="v1",k2="v2"} with keys sorted.
func groupKeyFor(groupLabels map[string]string) string {
	keys := make([]string, 0, len(groupLabels))
	for k := range groupLabels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(groupLabels[k])
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// groupLabelsFor projects an alert's labels onto the configured group_by set.
// A label absent from the alert is simply omitted from the group tuple.
func groupLabelsFor(lbls map[string]string, groupBy []string) map[string]string {
	out := make(map[string]string, len(groupBy))
	for _, name := range groupBy {
		if v, ok := lbls[name]; ok {
			out[name] = v
		}
	}
	return out
}

// commonPairs returns the key/value pairs present and identical across every
// input map (Alertmanager's commonLabels / commonAnnotations).
func commonPairs(maps []map[string]string) map[string]string {
	if len(maps) == 0 {
		return map[string]string{}
	}
	common := map[string]string{}
	for k, v := range maps[0] {
		common[k] = v
	}
	for _, m := range maps[1:] {
		for k, v := range common {
			if mv, ok := m[k]; !ok || mv != v {
				delete(common, k)
			}
		}
	}
	return common
}

// buildPayload assembles one Alertmanager v4 webhook body for a group's alerts.
// Firing alerts get an EndsAt of now+resolveTimeout so a receiver auto-resolves
// if refreshes stop; resolved alerts carry their real ResolvedAt.
func buildPayload(receiver, externalURL string, groupLabels map[string]string, alerts []Alert, now time.Time, resolveTimeout time.Duration) WebhookPayload {
	whAlerts := make([]WebhookAlert, 0, len(alerts))
	labelMaps := make([]map[string]string, 0, len(alerts))
	annMaps := make([]map[string]string, 0, len(alerts))
	anyFiring := false

	for _, a := range alerts {
		status := statusFiring
		var endsAt string
		if a.Resolved {
			status = statusResolved
			endsAt = a.ResolvedAt.UTC().Format(time.RFC3339)
		} else {
			anyFiring = true
			endsAt = now.Add(resolveTimeout).UTC().Format(time.RFC3339)
		}
		labels := a.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		annotations := a.Annotations
		if annotations == nil {
			annotations = map[string]string{}
		}
		whAlerts = append(whAlerts, WebhookAlert{
			Status:       status,
			Labels:       labels,
			Annotations:  annotations,
			StartsAt:     a.StartsAt.UTC().Format(time.RFC3339),
			EndsAt:       endsAt,
			GeneratorURL: a.GeneratorURL,
			Fingerprint:  fingerprint(labels),
		})
		labelMaps = append(labelMaps, labels)
		annMaps = append(annMaps, annotations)
	}

	status := statusResolved
	if anyFiring {
		status = statusFiring
	}

	return WebhookPayload{
		Version:           webhookVersion,
		GroupKey:          groupKeyFor(groupLabels),
		Status:            status,
		Receiver:          receiver,
		GroupLabels:       groupLabels,
		CommonLabels:      commonPairs(labelMaps),
		CommonAnnotations: commonPairs(annMaps),
		ExternalURL:       externalURL,
		Alerts:            whAlerts,
	}
}
