// Package logs parses log lines without guessing fields. By default (or when a
// line matches no known shape) it keeps the raw line verbatim in a normalized
// "message" column and extracts nothing else — logs are noisy, and inventing
// columns from free text produces high-cardinality garbage. Fields are only
// extracted for known formats: k8s (CRI container-log), json, syslog
// (RFC3164/5424), clf (Common Log Format), and cef (Common Event Format),
// selected explicitly via `format` or discovered per line with `format: auto`.
//
// Every row exposes a stable "message" (the templatable text) and a "format"
// column (which shape produced the row). Timestamp fields are never ingested:
// storage stamps its own ingest time, and an embedded per-line timestamp is a
// useless, high-cardinality summary dimension.
package logs

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/columnar"
	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this parser.
const Type = "logs"

// Format values.
const (
	formatNone   = "none"
	formatAuto   = "auto"
	formatK8s    = "k8s"
	formatJSON   = "json"
	formatSyslog = "syslog"
	formatCLF    = "clf"
	formatCEF    = "cef"
)

const defaultMessage = "message"

// formatColumn records which shape produced a row.
const formatColumn = "format"

// Config configures the logs parser.
type Config struct {
	// Format selects extraction: none (default) | auto | k8s | json | syslog |
	// clf | cef. "none" keeps the raw line; "auto" sniffs per line.
	Format string `json:"format"`
	// Message names the normalized templatable column (default "message").
	Message string `json:"message"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	switch c.Format {
	case "", formatNone, formatAuto, formatK8s, formatJSON, formatSyslog, formatCLF, formatCEF:
		return nil
	default:
		return fmt.Errorf("logs.format: must be one of none|auto|k8s|json|syslog|clf|cef, got %q", c.Format)
	}
}

func (c *Config) format() string {
	if c.Format == "" {
		return formatNone
	}
	return c.Format
}

func (c *Config) message() string {
	if c.Message == "" {
		return defaultMessage
	}
	return c.Message
}

type factory struct{}

// NewFactory returns the logs parser factory.
func NewFactory() component.Factory[component.Parser] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Parser, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("parser/logs: unexpected config type %T", cfg)
	}
	return &parser{format: c.format(), msg: c.message()}, nil
}

type parser struct {
	format string
	msg    string
	mem    memory.Allocator
}

func (p *parser) Start(_ context.Context, host component.Host) error {
	if host != nil {
		p.mem = host.Allocator()
	}
	if p.mem == nil {
		p.mem = memory.DefaultAllocator
	}
	return nil
}
func (p *parser) Shutdown(context.Context) error { return nil }

func (p *parser) Parse(_ context.Context, in data.RawBatch) (data.RecordBatch, error) {
	rows := make([]map[string]any, 0, len(in.Records))
	for _, rec := range in.Records {
		rows = append(rows, p.parseLine(strings.TrimRight(string(rec), "\r\n")))
	}
	return columnar.Build(p.mem, in.Source, rows)
}

// parseLine turns one line into a row. It picks the extractor by configured
// format; "auto" tries each known shape and falls back to raw.
func (p *parser) parseLine(line string) map[string]any {
	switch p.format {
	case formatK8s:
		if row, ok := parseK8s(line); ok {
			return p.finish(row, formatK8s)
		}
	case formatJSON:
		if row, ok := parseJSON(line); ok {
			return p.finish(row, formatJSON)
		}
	case formatSyslog:
		if row, ok := parseSyslog(line); ok {
			return p.finish(row, formatSyslog)
		}
	case formatCLF:
		if row, ok := parseCLF(line); ok {
			return p.finish(row, formatCLF)
		}
	case formatCEF:
		if row, ok := parseCEF(line); ok {
			return p.finish(row, formatCEF)
		}
	case formatAuto:
		if row, f, ok := discover(line); ok {
			return p.finish(row, f)
		}
	}
	// none, or a line that did not match its declared/discovered format.
	return p.finish(map[string]any{p.msg: line}, formatNone)
}

// finish stamps the message (under the configured column) and format columns.
// A per-format extractor returns its message under the "message" key; finish
// moves it to the configured column name.
func (p *parser) finish(row map[string]any, format string) map[string]any {
	if p.msg != defaultMessage {
		if m, ok := row[defaultMessage]; ok {
			delete(row, defaultMessage)
			row[p.msg] = m
		}
	}
	row[formatColumn] = format
	return row
}

// discover sniffs a line against the known formats in a cheap, unambiguous
// order and returns the first match.
func discover(line string) (map[string]any, string, bool) {
	switch trimmed := strings.TrimSpace(line); {
	case strings.HasPrefix(trimmed, "{"):
		if row, ok := parseJSON(line); ok {
			return row, formatJSON, true
		}
	case strings.HasPrefix(trimmed, "CEF:"):
		if row, ok := parseCEF(line); ok {
			return row, formatCEF, true
		}
	case strings.HasPrefix(trimmed, "<"):
		if row, ok := parseSyslog(line); ok {
			return row, formatSyslog, true
		}
	}
	if row, ok := parseK8s(line); ok {
		return row, formatK8s, true
	}
	if row, ok := parseCLF(line); ok {
		return row, formatCLF, true
	}
	return nil, formatNone, false
}

// timestampKeys are field names dropped from any known format: storage owns the
// ingest time and an embedded per-line timestamp is a high-cardinality noise
// column, useless as a summary dimension.
var timestampKeys = map[string]struct{}{
	"timestamp": {}, "time": {}, "ts": {}, "@timestamp": {},
	"date": {}, "datetime": {}, "eventtime": {}, "rt": {},
}

func isTimestampKey(k string) bool {
	_, ok := timestampKeys[strings.ToLower(k)]
	return ok
}

// ---- k8s (CRI container log) ------------------------------------------------
// <RFC3339Nano> <stdout|stderr> <F|P> <message>

func parseK8s(line string) (map[string]any, bool) {
	f := strings.SplitN(strings.TrimSpace(line), " ", 4)
	if len(f) < 4 {
		return nil, false
	}
	if f[1] != "stdout" && f[1] != "stderr" {
		return nil, false
	}
	if f[2] != "F" && f[2] != "P" {
		return nil, false
	}
	if !looksRFC3339(f[0]) {
		return nil, false
	}
	return map[string]any{
		"stream":       f[1],
		"logtag":       f[2],
		defaultMessage: f[3],
	}, true
}

// looksRFC3339 is a cheap shape check (date-T-time) — the value is dropped, so
// full validation is unnecessary; we only need to recognize the k8s shape.
func looksRFC3339(s string) bool {
	if len(s) < 20 || s[4] != '-' || s[7] != '-' || s[10] != 'T' {
		return false
	}
	return s[0] >= '0' && s[0] <= '9'
}

// ---- json -------------------------------------------------------------------

var jsonMessageKeys = []string{"message", "msg", "log", "body"}

func parseJSON(line string) (map[string]any, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "{") {
		return nil, false
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		return nil, false
	}
	row := make(map[string]any, len(obj))
	for k, raw := range obj {
		if isTimestampKey(k) {
			continue
		}
		row[k] = jsonScalar(raw)
	}
	// Normalize a message column from the first message-like key that holds a
	// string. The message column must always be a string so the downstream
	// template processor can mine it; a non-string message-like value is
	// ignored and we fall back to the raw line. Either branch assigns
	// defaultMessage, overwriting any non-string field that shares its name.
	found := false
	for _, k := range jsonMessageKeys {
		if s, isStr := row[k].(string); isStr {
			delete(row, k)
			row[defaultMessage] = s
			found = true
			break
		}
	}
	if !found {
		row[defaultMessage] = line
	}
	return row, true
}

func jsonScalar(raw json.RawMessage) any {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	switch x := v.(type) {
	case nil:
		return nil
	case bool:
		return x
	case float64:
		if x == float64(int64(x)) {
			return int64(x)
		}
		return x
	case string:
		return x
	default:
		return string(raw)
	}
}

// ---- syslog (RFC3164 / RFC5424) ---------------------------------------------

var (
	syslog3164Re = regexp.MustCompile(`^<(\d{1,3})>(\w{3}\s+\d+ \d{2}:\d{2}:\d{2}) (\S+) (.*)$`)
	syslog5424Re = regexp.MustCompile(`^<(\d{1,3})>1 (\S+) (\S+) (\S+) (\S+) (\S+) (.*)$`)
)

func parseSyslog(line string) (map[string]any, bool) {
	line = strings.TrimSpace(line)
	if m := syslog5424Re.FindStringSubmatch(line); m != nil {
		row := priFields(m[1])
		row["host"] = m[3]
		if m[4] != "-" {
			row["app"] = m[4]
		}
		if m[5] != "-" {
			row["procid"] = m[5]
		}
		if m[6] != "-" {
			row["msgid"] = m[6]
		}
		row[defaultMessage] = stripStructuredData(m[7])
		return row, true
	}
	if m := syslog3164Re.FindStringSubmatch(line); m != nil {
		row := priFields(m[1])
		row["host"] = m[3]
		app, msg := splitTag(m[4])
		if app != "" {
			row["app"] = app
		}
		row[defaultMessage] = msg
		return row, true
	}
	return nil, false
}

// priFields decodes the syslog PRI into facility and severity (RFC5424 §6.2.1).
func priFields(pri string) map[string]any {
	n := 0
	for _, r := range pri {
		n = n*10 + int(r-'0')
	}
	return map[string]any{
		"facility": int64(n / 8),
		"severity": int64(n % 8),
	}
}

// splitTag separates "app[pid]: message" or "app: message" into app and message.
func splitTag(rest string) (app, msg string) {
	i := strings.Index(rest, ": ")
	if i < 0 {
		return "", rest
	}
	tag := rest[:i]
	msg = rest[i+2:]
	if b := strings.IndexByte(tag, '['); b >= 0 {
		tag = tag[:b]
	}
	return tag, msg
}

// stripStructuredData drops a leading RFC5424 SD element ("-" or "[…]") so the
// message column holds only the free-text message.
func stripStructuredData(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "- ") {
		return s[2:]
	}
	if s == "-" {
		return ""
	}
	if strings.HasPrefix(s, "[") {
		if end := strings.Index(s, "] "); end >= 0 {
			return s[end+2:]
		}
	}
	return s
}

// ---- CLF (Common Log Format) ------------------------------------------------
// host ident authuser [date] "METHOD path proto" status size

var clfRe = regexp.MustCompile(`^(\S+) (\S+) (\S+) \[[^\]]+\] "([^"]*)" (\d{3}) (\d+|-)`)

func parseCLF(line string) (map[string]any, bool) {
	m := clfRe.FindStringSubmatch(strings.TrimSpace(line))
	if m == nil {
		return nil, false
	}
	row := map[string]any{
		"host":         m[1],
		"status":       m[5],
		defaultMessage: m[4],
	}
	if m[2] != "-" {
		row["ident"] = m[2]
	}
	if m[3] != "-" {
		row["authuser"] = m[3]
	}
	if m[6] != "-" {
		row["size"] = m[6]
	}
	if req := strings.Fields(m[4]); len(req) == 3 {
		row["method"], row["path"], row["protocol"] = req[0], req[1], req[2]
	}
	return row, true
}

// ---- CEF (Common Event Format) ----------------------------------------------
// CEF:Version|Vendor|Product|DeviceVersion|SignatureID|Name|Severity|Extension

var cefExtRe = regexp.MustCompile(`([A-Za-z][A-Za-z0-9_]*)=`)

func parseCEF(line string) (map[string]any, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "CEF:") {
		return nil, false
	}
	parts := strings.SplitN(line[len("CEF:"):], "|", 8)
	if len(parts) < 7 {
		return nil, false
	}
	row := map[string]any{
		"cef_version":    parts[0],
		"vendor":         parts[1],
		"product":        parts[2],
		"device_version": parts[3],
		"signature":      parts[4],
		"name":           parts[5],
		"severity":       parts[6],
		defaultMessage:   parts[5],
	}
	if len(parts) == 8 {
		for k, v := range parseCEFExtensions(parts[7]) {
			if !isTimestampKey(k) {
				row[k] = v
			}
		}
	}
	return row, true
}

// parseCEFExtensions splits "k1=v1 k2=v2 …" where values may contain spaces; a
// value runs until the next "key=" boundary.
func parseCEFExtensions(ext string) map[string]string {
	locs := cefExtRe.FindAllStringSubmatchIndex(ext, -1)
	out := make(map[string]string, len(locs))
	for i, loc := range locs {
		key := ext[loc[2]:loc[3]]
		valStart := loc[1]
		valEnd := len(ext)
		if i+1 < len(locs) {
			valEnd = locs[i+1][0]
		}
		out[key] = strings.TrimSpace(ext[valStart:valEnd])
	}
	return out
}
