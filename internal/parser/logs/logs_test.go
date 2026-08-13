package logs

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
)

// parseOne runs the parser over one line and returns the single row as a map of
// column name → value (string/int64/float64/bool/nil), for assertion.
func parseOne(t *testing.T, format, line string) map[string]any {
	t.Helper()
	return parseAll(t, format, []string{line})[0]
}

func parseAll(t *testing.T, format string, lines []string) []map[string]any {
	t.Helper()
	p, err := factory{}.Create(&Config{Format: format}, component.Settings{})
	if err != nil {
		t.Fatalf("Create(%q): %v", format, err)
	}
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	recs := make([][]byte, len(lines))
	for i, l := range lines {
		recs[i] = []byte(l)
	}
	rb, err := p.Parse(context.Background(), data.RawBatch{Source: "t", Records: recs})
	if err != nil {
		t.Fatalf("Parse(%q): %v", format, err)
	}
	defer rb.Release()
	return rowsOf(t, rb)
}

func rowsOf(t *testing.T, rb data.RecordBatch) []map[string]any {
	t.Helper()
	rec := rb.Record()
	if rec == nil {
		return nil
	}
	rows := make([]map[string]any, rec.NumRows())
	for i := range rows {
		rows[i] = map[string]any{}
	}
	for c := 0; c < int(rec.NumCols()); c++ {
		name := rec.Schema().Field(c).Name
		col := rec.Column(c)
		for r := 0; r < int(rec.NumRows()); r++ {
			if col.IsNull(r) {
				continue
			}
			switch a := col.(type) {
			case *array.String:
				rows[r][name] = a.Value(r)
			case *array.Int64:
				rows[r][name] = a.Value(r)
			case *array.Float64:
				rows[r][name] = a.Value(r)
			case *array.Boolean:
				rows[r][name] = a.Value(r)
			}
		}
	}
	return rows
}

func has(row map[string]any, col string) bool { _, ok := row[col]; return ok }

// no timestamp-like column may ever be ingested from a known format.
func assertNoTimestamp(t *testing.T, row map[string]any) {
	t.Helper()
	for _, k := range []string{"timestamp", "time", "ts", "@timestamp", "date", "datetime"} {
		if has(row, k) {
			t.Fatalf("timestamp-like column %q must be dropped; row=%v", k, row)
		}
	}
}

// none/unknown: the raw line is kept verbatim as message; no fields are guessed.
func TestParse_None(t *testing.T) {
	line := `level=info user=4821 msg="logged in" latency=12ms`
	row := parseOne(t, "none", line)
	if row["message"] != line {
		t.Fatalf("message = %v, want the raw line", row["message"])
	}
	if row["format"] != "none" {
		t.Fatalf("format = %v, want none", row["format"])
	}
	// No field guessing: only message + format columns.
	for k := range row {
		if k != "message" && k != "format" {
			t.Fatalf("none must not extract fields, found %q", k)
		}
	}
}

func TestParse_K8sCRI(t *testing.T) {
	row := parseOne(t, "k8s", `2026-07-04T00:11:22.123456789Z stdout F user 12 logged in`)
	if row["message"] != "user 12 logged in" {
		t.Fatalf("message = %v", row["message"])
	}
	if row["stream"] != "stdout" || row["logtag"] != "F" {
		t.Fatalf("stream/logtag wrong: %v", row)
	}
	if row["format"] != "k8s" {
		t.Fatalf("format = %v, want k8s", row["format"])
	}
	assertNoTimestamp(t, row)
}

func TestParse_JSON_DropsTimestampAndFindsMessage(t *testing.T) {
	row := parseOne(t, "json", `{"level":"error","msg":"disk full","code":507,"ts":"2026-07-04T00:00:00Z"}`)
	if row["level"] != "error" || row["message"] != "disk full" {
		t.Fatalf("fields wrong: %v", row)
	}
	if row["code"] != int64(507) {
		t.Fatalf("code = %v, want int64 507", row["code"])
	}
	assertNoTimestamp(t, row)
}

func TestParse_Syslog5424(t *testing.T) {
	row := parseOne(t, "syslog", `<34>1 2026-07-04T00:00:00.003Z host.example.com su 1234 ID47 - the message body`)
	if row["host"] != "host.example.com" || row["app"] != "su" {
		t.Fatalf("host/app wrong: %v", row)
	}
	if row["procid"] != int64(1234) || row["msgid"] != "ID47" {
		t.Fatalf("procid/msgid wrong: %v", row)
	}
	if row["message"] != "the message body" {
		t.Fatalf("message = %v (structured-data '-' not stripped?)", row["message"])
	}
	assertNoTimestamp(t, row)
}

// A message-like key whose value is not a string must not become the message
// column (that would type it as int and break templating); fall back to the raw
// line, which is always a string.
func TestParse_JSON_NonStringMessageFallsBackToLine(t *testing.T) {
	line := `{"message":123,"level":"warn"}`
	row := parseOne(t, "json", line)
	if _, ok := row["message"].(string); !ok {
		t.Fatalf("message must be a string, got %T (%v)", row["message"], row["message"])
	}
	if row["message"] != line {
		t.Fatalf("message = %v, want raw line fallback", row["message"])
	}
	if row["level"] != "warn" {
		t.Fatalf("level lost: %v", row)
	}
}

// A null primary message key should be skipped in favor of the next string key.
func TestParse_JSON_NullMessageUsesNextKey(t *testing.T) {
	row := parseOne(t, "json", `{"message":null,"msg":"hello"}`)
	if row["message"] != "hello" {
		t.Fatalf("message = %v, want the string from msg", row["message"])
	}
}

// auto mode must detect JSON even with leading whitespace (parity with explicit).
func TestParse_AutoJSONLeadingWhitespace(t *testing.T) {
	row := parseOne(t, "auto", "   "+`{"msg":"ok","n":1}`)
	if row["format"] != "json" || row["message"] != "ok" || row["n"] != int64(1) {
		t.Fatalf("leading-whitespace json not detected: %v", row)
	}
}

func TestParse_Syslog3164(t *testing.T) {
	row := parseOne(t, "syslog", `<34>Oct 11 22:14:15 mymachine su: 'su root' failed for user`)
	if row["host"] != "mymachine" || row["app"] != "su" {
		t.Fatalf("host/app wrong: %v", row)
	}
	if row["severity"] != int64(2) || row["facility"] != int64(4) {
		t.Fatalf("pri decode wrong: %v", row)
	}
	if row["message"] != "'su root' failed for user" {
		t.Fatalf("message = %v", row["message"])
	}
	assertNoTimestamp(t, row)
}

func TestParse_CLF(t *testing.T) {
	row := parseOne(t, "clf", `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`)
	if row["host"] != "127.0.0.1" || row["authuser"] != "frank" {
		t.Fatalf("host/user wrong: %v", row)
	}
	if row["method"] != "GET" || row["path"] != "/apache_pb.gif" || row["protocol"] != "HTTP/1.0" {
		t.Fatalf("request parse wrong: %v", row)
	}
	if row["status"] != int64(200) || row["size"] != int64(2326) {
		t.Fatalf("status/size wrong: %v", row)
	}
	assertNoTimestamp(t, row)
}

func TestParse_CEF(t *testing.T) {
	row := parseOne(t, "cef", `CEF:0|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232`)
	if row["vendor"] != "Security" || row["product"] != "threatmanager" {
		t.Fatalf("header wrong: %v", row)
	}
	if row["name"] != "worm successfully stopped" || row["message"] != "worm successfully stopped" {
		t.Fatalf("name/message wrong: %v", row)
	}
	if row["src"] != "10.0.0.1" || row["dst"] != "2.1.2.2" {
		t.Fatalf("extensions wrong: %v", row)
	}
}

// auto: each line is sniffed; a JSON line yields fields, a plain line stays raw.
func TestParse_AutoMixed(t *testing.T) {
	rows := parseAll(t, "auto", []string{
		`{"level":"info","msg":"ok"}`,
		`just a plain line 42`,
	})
	if rows[0]["level"] != "info" || rows[0]["message"] != "ok" || rows[0]["format"] != "json" {
		t.Fatalf("json row wrong: %v", rows[0])
	}
	if rows[1]["message"] != "just a plain line 42" || rows[1]["format"] != "none" {
		t.Fatalf("plain row wrong: %v", rows[1])
	}
}

// auto falls back to none when a line does not match any known format.
func TestParse_AutoFallbackNone(t *testing.T) {
	row := parseOne(t, "auto", `this is not any known format`)
	if row["format"] != "none" || row["message"] != "this is not any known format" {
		t.Fatalf("fallback wrong: %v", row)
	}
}

// An empty batch parses to an empty batch without error.
func TestParse_EmptyBatch(t *testing.T) {
	p, _ := factory{}.Create(&Config{Format: "auto"}, component.Settings{})
	_ = p.Start(context.Background(), nil)
	rb, err := p.Parse(context.Background(), data.RawBatch{Source: "t"})
	if err != nil {
		t.Fatalf("Parse empty: %v", err)
	}
	defer rb.Release()
	if rb.Len() != 0 {
		t.Fatalf("empty batch produced %d rows", rb.Len())
	}
}

// A line that does not match its explicitly declared format falls back to raw
// (message = line, format = none) rather than erroring or half-parsing.
func TestParse_DeclaredFormatMismatchFallsBack(t *testing.T) {
	for _, f := range []string{"k8s", "syslog", "clf", "cef", "json"} {
		row := parseOne(t, f, "totally unstructured text 7")
		if row["format"] != "none" || row["message"] != "totally unstructured text 7" {
			t.Fatalf("format %q mismatch should fall back to raw, got %v", f, row)
		}
	}
}

func TestConfig_Validate(t *testing.T) {
	if err := (&Config{Format: "bogus"}).Validate(); err == nil {
		t.Fatal("bogus format should be rejected")
	}
	for _, f := range []string{"", "none", "auto", "k8s", "json", "syslog", "clf", "cef"} {
		if err := (&Config{Format: f}).Validate(); err != nil {
			t.Fatalf("format %q should be valid: %v", f, err)
		}
	}
}

// message column name is configurable.
func TestParse_CustomMessageColumn(t *testing.T) {
	p, _ := factory{}.Create(&Config{Format: "none", Message: "line"}, component.Settings{})
	_ = p.Start(context.Background(), nil)
	rb, err := p.Parse(context.Background(), data.RawBatch{Source: "t", Records: [][]byte{[]byte("hello")}})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	defer rb.Release()
	row := rowsOf(t, rb)[0]
	if row["line"] != "hello" || has(row, "message") {
		t.Fatalf("custom message column not honored: %v", row)
	}
}

func parseSource(t *testing.T, format, source, line string, labels map[string]string) map[string]any {
	t.Helper()
	p, err := factory{}.Create(&Config{Format: format}, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	rb, err := p.Parse(context.Background(), data.RawBatch{
		Source:  source,
		Records: [][]byte{[]byte(line)},
		Labels:  labels,
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	defer rb.Release()
	rows := rowsOf(t, rb)
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	return rows[0]
}

func TestParse_K8sPodsPathEnrichment(t *testing.T) {
	src := "/var/log/pods/user-fknjdouh-apps_prism-cache-abc_01234567-89ab-cdef-0123-456789abcdef/store/0.log"
	row := parseSource(t, "k8s", src, `2026-07-04T00:11:22.123456789Z stdout F boom`, nil)
	if row["namespace"] != "user-fknjdouh-apps" || row["pod"] != "prism-cache-abc" || row["container"] != "store" {
		t.Fatalf("k8s identity = ns=%v pod=%v container=%v", row["namespace"], row["pod"], row["container"])
	}
	if has(row, "path") || has(row, "filename") || has(row, "uid") {
		t.Fatalf("must not label path/uid: %v", row)
	}
}

func TestParse_K8sContainersSymlinkEnrichment(t *testing.T) {
	id := strings.Repeat("a", 64)
	src := "/var/log/containers/prism-cache-abc_user-fknjdouh-apps_store-" + id + ".log"
	row := parseSource(t, "auto", src, `2026-07-04T00:11:22.123456789Z stderr F oops`, nil)
	if row["namespace"] != "user-fknjdouh-apps" || row["pod"] != "prism-cache-abc" || row["container"] != "store" {
		t.Fatalf("containers identity = ns=%v pod=%v container=%v row=%v", row["namespace"], row["pod"], row["container"], row)
	}
}

func TestParse_NonK8sSourceNoIdentity(t *testing.T) {
	row := parseSource(t, "none", "/var/log/app/service.log", "hello", nil)
	for _, k := range []string{"namespace", "pod", "container"} {
		if has(row, k) {
			t.Fatalf("non-k8s source must not invent %q: %v", k, row)
		}
	}
}

func TestParse_PathEnrichmentHonorLabels(t *testing.T) {
	src := "/var/log/pods/ns-a_pod-a_01234567-89ab-cdef-0123-456789abcdef/c-a/0.log"
	row := parseSource(t, "k8s", src, `2026-07-04T00:11:22.123456789Z stdout F x`, map[string]string{
		"namespace": "from-label",
		"pod":       "from-label-pod",
	})
	if row["namespace"] != "from-label" || row["pod"] != "from-label-pod" {
		t.Fatalf("RawBatch.Labels must win: %v", row)
	}
	if row["container"] != "c-a" {
		t.Fatalf("path container should fill when label absent: %v", row)
	}
}

func TestParse_JSONLineFieldsWinOverPath(t *testing.T) {
	src := "/var/log/pods/ns-a_pod-a_01234567-89ab-cdef-0123-456789abcdef/c-a/0.log"
	row := parseSource(t, "json", src, `{"message":"hi","namespace":"from-json","pod":"from-json"}`, nil)
	if row["namespace"] != "from-json" || row["pod"] != "from-json" {
		t.Fatalf("parsed fields must win over path: %v", row)
	}
	if row["container"] != "c-a" {
		t.Fatalf("path container should fill when line omits it: %v", row)
	}
}

func TestParseK8sLogPath(t *testing.T) {
	cases := []struct {
		name       string
		path       string
		ns, pod, c string
		ok         bool
	}{
		{"pods", "/var/log/pods/default_nginx-7f5c_01234567-89ab-cdef-0123-456789abcdef/nginx/0.log", "default", "nginx-7f5c", "nginx", true},
		{"pods_relative", "pods/kube-system_coredns_01234567-89ab-cdef-0123-456789abcdef/coredns/1.log", "kube-system", "coredns", "coredns", true},
		{"containers", "/var/log/containers/nginx-7f5c_default_nginx-" + strings.Repeat("b", 64) + ".log", "default", "nginx-7f5c", "nginx", true},
		{"containers_hyphenated_name", "/var/log/containers/x_y_my-sidecar-" + strings.Repeat("c", 64) + ".log", "y", "x", "my-sidecar", true},
		{"plain_file", "/var/log/syslog", "", "", "", false},
		{"truncated_container_id", "/var/log/containers/p_n_c-abcd.log", "", "", "", false},
		{"pods_missing_container", "/var/log/pods/ns_pod_01234567-89ab-cdef-0123-456789abcdef/0.log", "", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns, pod, c, ok := parseK8sLogPath(tc.path)
			if ok != tc.ok {
				t.Fatalf("ok=%v want %v (%q %q %q)", ok, tc.ok, ns, pod, c)
			}
			if !tc.ok {
				return
			}
			if ns != tc.ns || pod != tc.pod || c != tc.c {
				t.Fatalf("got ns=%q pod=%q c=%q want %q %q %q", ns, pod, c, tc.ns, tc.pod, tc.c)
			}
		})
	}
}
