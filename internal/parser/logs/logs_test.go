package logs

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
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
