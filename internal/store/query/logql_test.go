package query

import (
	"errors"
	"testing"
)

func TestParseLogQLMatchAll(t *testing.T) {
	// Grafana Explore sends an empty query while the user is still typing, and a
	// bare selector is the "show me everything" query. Both must match all logs.
	for _, expr := range []string{"", "   ", "{}", "{ }"} {
		q, err := parseLogQL(expr)
		if err != nil {
			t.Fatalf("parseLogQL(%q) error: %v", expr, err)
		}
		if len(q.matchers) != 0 || len(q.filters) != 0 {
			t.Fatalf("parseLogQL(%q) = %+v, want match-all", expr, q)
		}
	}
}

func TestParseLogQLStreamSelector(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want []logQLMatcher
	}{
		{"equal", `{format="json"}`, []logQLMatcher{{label: "format", op: logQLEqual, value: "json"}}},
		{"not_equal", `{format!="json"}`, []logQLMatcher{{label: "format", op: logQLNotEqual, value: "json"}}},
		{"regex", `{format=~"js.*"}`, []logQLMatcher{{label: "format", op: logQLMatchRegex, value: "js.*"}}},
		{"not_regex", `{format!~"js.*"}`, []logQLMatcher{{label: "format", op: logQLNotMatchRegex, value: "js.*"}}},
		{"two_labels", `{job="prism", format="none"}`, []logQLMatcher{
			{label: "job", op: logQLEqual, value: "prism"},
			{label: "format", op: logQLEqual, value: "none"},
		}},
		{"backtick_value", "{format=`json`}", []logQLMatcher{{label: "format", op: logQLEqual, value: "json"}}},
		{"escaped_quote", `{template="say \"hi\""}`, []logQLMatcher{{label: "template", op: logQLEqual, value: `say "hi"`}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := parseLogQL(tc.expr)
			if err != nil {
				t.Fatalf("parseLogQL(%q) error: %v", tc.expr, err)
			}
			if len(q.matchers) != len(tc.want) {
				t.Fatalf("matchers = %+v, want %+v", q.matchers, tc.want)
			}
			for i, w := range tc.want {
				got := q.matchers[i]
				if got.label != w.label || got.op != w.op || got.value != w.value {
					t.Fatalf("matcher %d = %+v, want %+v", i, got, w)
				}
			}
		})
	}
}

func TestParseLogQLLineFilters(t *testing.T) {
	q, err := parseLogQL(`{job="prism"} |= "disk" != "sda2" |~ "d.sk" !~ "^ok$"`)
	if err != nil {
		t.Fatalf("parseLogQL error: %v", err)
	}
	want := []logQLLineFilter{
		{op: logQLEqual, value: "disk"},
		{op: logQLNotEqual, value: "sda2"},
		{op: logQLMatchRegex, value: "d.sk"},
		{op: logQLNotMatchRegex, value: "^ok$"},
	}
	if len(q.filters) != len(want) {
		t.Fatalf("filters = %+v, want %+v", q.filters, want)
	}
	for i, w := range want {
		if q.filters[i].op != w.op || q.filters[i].value != w.value {
			t.Fatalf("filter %d = %+v, want %+v", i, q.filters[i], w)
		}
	}
}

// TestParseLogQLLineFilterWithoutSelector accepts a bare line filter, so a user
// who types only `|= "text"` still gets a query over every stream.
func TestParseLogQLLineFilterWithoutSelector(t *testing.T) {
	q, err := parseLogQL(`{} |= "disk"`)
	if err != nil {
		t.Fatalf("parseLogQL error: %v", err)
	}
	if len(q.matchers) != 0 || len(q.filters) != 1 || q.filters[0].value != "disk" {
		t.Fatalf("query = %+v", q)
	}
}

func TestParseLogQLUnsupported(t *testing.T) {
	cases := map[string]string{
		"rate":            `rate({job="prism"}[5m])`,
		"count_over_time": `count_over_time({job="prism"}[1m])`,
		"aggregation":     `sum by (job) (rate({job="prism"}[5m]))`,
		"range_vector":    `{job="prism"}[5m]`,
		"json_parser":     `{job="prism"} | json`,
		"logfmt_parser":   `{job="prism"} | logfmt`,
		"label_filter":    `{job="prism"} | count > 2`,
		"line_format":     `{job="prism"} | line_format "{{.message}}"`,
		"unwrap":          `sum(sum_over_time({job="prism"} | unwrap count [5m]))`,
	}
	for name, expr := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseLogQL(expr)
			if !errors.Is(err, errUnsupportedLogQL) {
				t.Fatalf("parseLogQL(%q) err = %v, want errUnsupportedLogQL", expr, err)
			}
		})
	}
}

func TestParseLogQLMalformed(t *testing.T) {
	cases := map[string]string{
		"unterminated_selector": `{job="prism"`,
		"missing_value":         `{job=}`,
		"missing_label":         `{="prism"}`,
		"invalid_label_name":    `{job-name="prism"}`,
		"unquoted_value":        `{job=prism}`,
		"missing_operator":      `{job "prism"}`,
		"unterminated_value":    `{job="prism}`,
		"bad_label_regex":       `{job=~"("}`,
		"filter_without_value":  `{job="prism"} |=`,
		"bad_filter_regex":      `{job="prism"} |~ "("`,
		"trailing_comma":        `{job="prism",}`,
	}
	for name, expr := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseLogQL(expr); err == nil {
				t.Fatalf("parseLogQL(%q) accepted a malformed query", expr)
			}
		})
	}
}

func TestLogQLMatcherMatches(t *testing.T) {
	cases := []struct {
		name    string
		matcher logQLMatcher
		value   string
		want    bool
	}{
		{"equal_hit", mustMatcher(t, "format", logQLEqual, "json"), "json", true},
		{"equal_miss", mustMatcher(t, "format", logQLEqual, "json"), "none", false},
		{"not_equal_hit", mustMatcher(t, "format", logQLNotEqual, "json"), "none", true},
		{"not_equal_miss", mustMatcher(t, "format", logQLNotEqual, "json"), "json", false},
		{"regex_anchored_hit", mustMatcher(t, "format", logQLMatchRegex, "js.n"), "json", true},
		{"regex_anchored_miss", mustMatcher(t, "format", logQLMatchRegex, "js"), "json", false},
		{"not_regex_hit", mustMatcher(t, "format", logQLNotMatchRegex, "js.n"), "none", true},
		{"absent_label_equal_empty", mustMatcher(t, "nope", logQLEqual, ""), "", true},
		{"absent_label_not_equal", mustMatcher(t, "nope", logQLNotEqual, "x"), "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.matcher.matches(tc.value); got != tc.want {
				t.Fatalf("matches(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

func mustMatcher(t *testing.T, label string, op logQLMatchOp, value string) logQLMatcher {
	t.Helper()
	m, err := newLogQLMatcher(label, op, value)
	if err != nil {
		t.Fatalf("newLogQLMatcher: %v", err)
	}
	return m
}
