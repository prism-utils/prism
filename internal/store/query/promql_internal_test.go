package query

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

func TestPromQLConfigValidate(t *testing.T) {
	base := PromQLConfig{MaxSamples: 10, Timeout: time.Second, LookbackDelta: time.Minute, MaxPoints: 10}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := map[string]func(*PromQLConfig){
		"promql.max_samples":    func(c *PromQLConfig) { c.MaxSamples = 0 },
		"promql.timeout":        func(c *PromQLConfig) { c.Timeout = 0 },
		"promql.lookback_delta": func(c *PromQLConfig) { c.LookbackDelta = 0 },
		"promql.max_points":     func(c *PromQLConfig) { c.MaxPoints = 0 },
	}
	for wantPath, mutate := range cases {
		cfg := base
		mutate(&cfg)
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), wantPath) {
			t.Errorf("mutation for %q: err = %v, want it to name the path", wantPath, err)
		}
	}
}

func TestPromQLConfigFromEnvDefaults(t *testing.T) {
	cfg := PromQLConfigFromEnv("/data", "/p", "512MB", 4)
	if cfg.MaxSamples != defaultPromQLMaxSamples || cfg.Timeout != defaultPromQLTimeout ||
		cfg.LookbackDelta != defaultPromQLLookbackDelta || cfg.MaxPoints != defaultPromQLMaxPoints {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	if cfg.MemoryLimit != "512MB" || cfg.Threads != 4 || cfg.DataDir != "/data" {
		t.Fatalf("passthrough fields wrong: %+v", cfg)
	}
}

func TestPromQLConfigFromEnvOverrides(t *testing.T) {
	t.Setenv("PROMQL_MAX_SAMPLES", "123")
	t.Setenv("PROMQL_TIMEOUT_SECONDS", "7")
	t.Setenv("PROMQL_LOOKBACK_DELTA_SECONDS", "60")
	t.Setenv("PROMQL_MAX_POINTS", "9")
	cfg := PromQLConfigFromEnv("/d", "", "", 0)
	if cfg.MaxSamples != 123 || cfg.Timeout != 7*time.Second ||
		cfg.LookbackDelta != 60*time.Second || cfg.MaxPoints != 9 {
		t.Fatalf("env overrides not applied: %+v", cfg)
	}
}

func TestPromQLAPIEnabledFromEnv(t *testing.T) {
	if !PromQLAPIEnabledFromEnv() {
		t.Fatal("default should be enabled")
	}
	t.Setenv("PROMQL_API_ENABLED", "false")
	if PromQLAPIEnabledFromEnv() {
		t.Fatal("false should disable")
	}
	t.Setenv("PROMQL_API_ENABLED", "garbage")
	if !PromQLAPIEnabledFromEnv() {
		t.Fatal("unparsable should fall back to enabled")
	}
}

func TestPromQLRoutePatterns(t *testing.T) {
	got := PromQLRoutePatterns("/prefix")
	if len(got) != 9 {
		t.Fatalf("pattern count = %d, want 9: %v", len(got), got)
	}
	wantSome := []string{
		"GET /prefix/{ns}/api/v1/query",
		"POST /prefix/{ns}/api/v1/query_range",
		"GET /prefix/{ns}/api/v1/label/{name}/values",
	}
	for _, w := range wantSome {
		found := false
		for _, p := range got {
			if p == w {
				found = true
			}
		}
		if !found {
			t.Errorf("missing pattern %q in %v", w, got)
		}
	}
	// No POST for label values (Prometheus does not define it).
	for _, p := range got {
		if p == "POST /prefix/{ns}/api/v1/label/{name}/values" {
			t.Errorf("unexpected POST label values pattern")
		}
	}
}

func TestParseSeriesLabels(t *testing.T) {
	lbls, err := parseSeriesLabels("up", `job="api",instance="a:9100"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if lbls.Get("__name__") != "up" || lbls.Get("job") != "api" || lbls.Get("instance") != "a:9100" {
		t.Fatalf("labels wrong: %s", lbls.String())
	}
	// Sorted order: __name__, instance, job.
	var names []string
	lbls.Range(func(l labels.Label) { names = append(names, l.Name) })
	if strings.Join(names, ",") != "__name__,instance,job" {
		t.Fatalf("not sorted: %v", names)
	}
}

func TestParseSeriesLabelsEmpty(t *testing.T) {
	lbls, err := parseSeriesLabels("m", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if lbls.Len() != 1 || lbls.Get("__name__") != "m" {
		t.Fatalf("empty labels wrong: %s", lbls.String())
	}
}

func TestForEachLabelPairEscapes(t *testing.T) {
	got := map[string]string{}
	if err := forEachLabelPair(`a="x\"y",b="c\\d",c="e\nf"`, func(k, v string) { got[k] = v }); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got["a"] != `x"y` || got["b"] != `c\d` || got["c"] != "e\nf" {
		t.Fatalf("escapes wrong: %#v", got)
	}
}

func TestForEachLabelPairMalformed(t *testing.T) {
	bad := []string{`novalue`, `k=unquoted`, `k="unterminated`}
	for _, s := range bad {
		if err := forEachLabelPair(s, func(string, string) {}); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}

func TestBuildSelectSQLNamePushdown(t *testing.T) {
	m := labels.MustNewMatcher(labels.MatchEqual, metricNameLabel, "up")
	sqlText, args := buildSelectSQL("metrics", 100, 200, []*labels.Matcher{m})
	if !strings.Contains(sqlText, `"__name__" = ?`) {
		t.Fatalf("missing name pushdown: %s", sqlText)
	}
	if !strings.Contains(sqlText, "ORDER BY") {
		t.Fatalf("missing order by: %s", sqlText)
	}
	if len(args) != 3 || args[2] != "up" {
		t.Fatalf("args wrong: %v", args)
	}
}

func TestBuildSelectSQLNoNamePushdown(t *testing.T) {
	m := labels.MustNewMatcher(labels.MatchRegexp, "job", "a.*")
	sqlText, args := buildSelectSQL("metrics", 1, 2, []*labels.Matcher{m})
	if strings.Contains(sqlText, `"__name__" = ?`) {
		t.Fatalf("must not push regex/non-name matcher: %s", sqlText)
	}
	if len(args) != 2 {
		t.Fatalf("args wrong: %v", args)
	}
}

func TestMatchesAll(t *testing.T) {
	lbls, _ := parseSeriesLabels("up", `job="api"`)
	eq := labels.MustNewMatcher(labels.MatchEqual, "job", "api")
	ne := labels.MustNewMatcher(labels.MatchNotEqual, "job", "api")
	if !matchesAll(lbls, []*labels.Matcher{eq}) {
		t.Fatal("eq should match")
	}
	if matchesAll(lbls, []*labels.Matcher{ne}) {
		t.Fatal("ne should not match")
	}
}

func TestFormatValue(t *testing.T) {
	cases := map[float64]string{
		1:            "1",
		0:            "0",
		0.5:          "0.5",
		math.NaN():   "NaN",
		math.Inf(1):  "+Inf",
		math.Inf(-1): "-Inf",
		123456.789:   "123456.789",
	}
	for in, want := range cases {
		if got := formatValue(in); got != want {
			t.Errorf("formatValue(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestTsSeconds(t *testing.T) {
	if tsSeconds(1500) != 1.5 {
		t.Fatalf("tsSeconds(1500) = %v", tsSeconds(1500))
	}
}

func TestParseTimeParam(t *testing.T) {
	def := time.Unix(42, 0).UTC()
	if got, _ := parseTimeParam("", def); !got.Equal(def) {
		t.Fatalf("empty should return default")
	}
	got, err := parseTimeParam("1435781451.781", time.Time{})
	if err != nil || got.Unix() != 1435781451 {
		t.Fatalf("unix float parse: %v %v", got, err)
	}
	got, err = parseTimeParam("2015-07-01T20:10:51Z", time.Time{})
	if err != nil || got.Unix() != 1435781451 {
		t.Fatalf("rfc3339 parse: %v %v", got, err)
	}
	if _, err := parseTimeParam("not-a-time", time.Time{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseDurationParam(t *testing.T) {
	if d, _ := parseDurationParam("15s"); d != 15*time.Second {
		t.Fatalf("15s = %v", d)
	}
	if d, _ := parseDurationParam("30"); d != 30*time.Second {
		t.Fatalf("30 = %v", d)
	}
	if _, err := parseDurationParam(""); err == nil {
		t.Fatal("empty should error")
	}
}

func TestIsValidLabelName(t *testing.T) {
	valid := []string{"job", "__name__", "a1", "_x"}
	invalid := []string{"", "1abc", "a-b", "a.b", "a b"}
	for _, v := range valid {
		if !isValidLabelName(v) {
			t.Errorf("%q should be valid", v)
		}
	}
	for _, v := range invalid {
		if isValidLabelName(v) {
			t.Errorf("%q should be invalid", v)
		}
	}
}
