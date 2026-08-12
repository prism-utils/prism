// Package prometheus parses the Prometheus text exposition format (one sample
// per line) into a stable, columnar RecordBatch:
//
//	__name__ (string) | labels (string) | value (float64) | timestamp_ms (int64)
//
// The schema is fixed so windows of samples concatenate cleanly in the buffer
// and group_by:["__name__"] summaries work directly. Comment (#) and blank
// lines are skipped. The parser never panics on malformed input; a malformed
// line yields an error the runtime routes per the failure policy.
package prometheus

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
)

// Type is the config identifier for this parser.
const Type = "prometheus"

// Column names of the fixed sample schema.
const (
	ColName      = "__name__"
	ColLabels    = "labels"
	ColValue     = "value"
	ColTimestamp = "timestamp_ms"
)

// schema is the fixed output schema shared by every batch this parser emits.
var schema = arrow.NewSchema([]arrow.Field{
	{Name: ColName, Type: arrow.BinaryTypes.String},
	{Name: ColLabels, Type: arrow.BinaryTypes.String},
	{Name: ColValue, Type: arrow.PrimitiveTypes.Float64},
	{Name: ColTimestamp, Type: arrow.PrimitiveTypes.Int64},
}, nil)

// Config configures the prometheus parser (no options).
type Config struct{}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns the prometheus parser factory.
func NewFactory() component.Factory[component.Parser] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Parser, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("parser/prometheus: unexpected config type %T", cfg)
	}
	return &parser{}, nil
}

type parser struct{ mem memory.Allocator }

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

// sample is one parsed exposition line.
type sample struct {
	name   string
	labels string
	value  float64
	tsMs   int64
}

func (p *parser) Parse(_ context.Context, in data.RawBatch) (data.RecordBatch, error) {
	nameB := array.NewStringBuilder(p.mem)
	labelsB := array.NewStringBuilder(p.mem)
	valueB := array.NewFloat64Builder(p.mem)
	tsB := array.NewInt64Builder(p.mem)
	defer nameB.Release()
	defer labelsB.Release()
	defer valueB.Release()
	defer tsB.Release()

	// Precompute the sorted keys of the batch's producer-supplied labels once so
	// per-sample merging is deterministic and allocation-light.
	baseKeys := sortedKeys(in.Labels)

	n := 0
	for _, rec := range in.Records {
		line := strings.TrimSpace(string(rec))
		if line == "" || line[0] == '#' {
			continue
		}
		s, err := parseLine(line)
		if err != nil {
			return data.RecordBatch{}, fmt.Errorf("parser/prometheus: %w", err)
		}
		s.labels = mergeLabels(s.labels, in.Labels, baseKeys)
		nameB.Append(s.name)
		labelsB.Append(s.labels)
		valueB.Append(s.value)
		tsB.Append(s.tsMs)
		n++
	}

	cols := []arrow.Array{nameB.NewArray(), labelsB.NewArray(), valueB.NewArray(), tsB.NewArray()}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	rec := array.NewRecordBatch(schema, cols, int64(n))
	return data.NewRecordBatch(in.Source, rec), nil
}

// sortedKeys returns the keys of m in ascending order, or nil when m is empty.
func sortedKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// mergeLabels appends producer-supplied labels to an exposition label block,
// preserving any label the sample already defines (honor_labels semantics).
// sortedKeys must list base's keys in ascending order for deterministic output.
// Returns existing unchanged when there is nothing to merge.
func mergeLabels(existing string, base map[string]string, sortedKeys []string) string {
	if len(base) == 0 {
		return existing
	}
	present := existingLabelKeys(existing)
	var sb strings.Builder
	sb.WriteString(existing)
	for _, k := range sortedKeys {
		if _, ok := present[k]; ok {
			continue // sample's own label wins
		}
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteString(`="`)
		writeEscapedValue(&sb, base[k])
		sb.WriteByte('"')
	}
	return sb.String()
}

// writeEscapedValue writes v as a quoted exposition label value body, escaping
// backslash, double-quote, and newline per the text format.
func writeEscapedValue(sb *strings.Builder, v string) {
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case '\\':
			sb.WriteString(`\\`)
		case '"':
			sb.WriteString(`\"`)
		case '\n':
			sb.WriteString(`\n`)
		default:
			sb.WriteByte(v[i])
		}
	}
}

// existingLabelKeys returns the set of label names present in an exposition
// label block (`k="v",k2="v2"`), tolerating whitespace and honoring quoted
// values with `\"` / `\\` escapes so commas inside values are not mistaken for
// separators.
func existingLabelKeys(s string) map[string]struct{} {
	keys := make(map[string]struct{})
	i, n := 0, len(s)
	skipSpace := func() {
		for i < n && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
	}
	for {
		skipSpace()
		if i >= n {
			return keys
		}
		start := i
		for i < n && s[i] != '=' && s[i] != ',' && s[i] != ' ' && s[i] != '\t' {
			i++
		}
		key := s[start:i]
		skipSpace()
		if i < n && s[i] == '=' {
			i++
			skipSpace()
			if i < n && s[i] == '"' {
				i++ // opening quote
				for i < n {
					if s[i] == '\\' {
						i += 2
						continue
					}
					if s[i] == '"' {
						i++
						break
					}
					i++
				}
			} else {
				for i < n && s[i] != ',' {
					i++
				}
			}
		}
		if key != "" {
			keys[key] = struct{}{}
		}
		skipSpace()
		if i < n && s[i] == ',' {
			i++
		}
	}
}

// parseLine parses one exposition line: name[{labels}] value [timestamp].
func parseLine(line string) (sample, error) {
	var s sample
	i := 0
	for i < len(line) && line[i] != '{' && line[i] != ' ' && line[i] != '\t' {
		i++
	}
	s.name = line[:i]
	if s.name == "" {
		return s, fmt.Errorf("empty metric name in %q", line)
	}
	rest := line[i:]
	if strings.HasPrefix(rest, "{") {
		end, err := labelBlockEnd(rest)
		if err != nil {
			return s, fmt.Errorf("%w in %q", err, line)
		}
		s.labels = rest[1 : end-1] // inside the braces, canonical form kept as-is
		rest = rest[end:]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return s, fmt.Errorf("missing value in %q", line)
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return s, fmt.Errorf("bad value %q in %q", fields[0], line)
	}
	s.value = v
	if len(fields) >= 2 {
		ts, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return s, fmt.Errorf("bad timestamp %q in %q", fields[1], line)
		}
		s.tsMs = ts
	}
	return s, nil
}

// labelBlockEnd returns the index just past the matching '}' of a label block
// starting at s[0]=='{', respecting quoted values and backslash escapes.
func labelBlockEnd(s string) (int, error) {
	inQuote := false
	escaped := false
	for i := 1; i < len(s); i++ {
		c := s[i]
		switch {
		case escaped:
			escaped = false
		case c == '\\':
			escaped = true
		case c == '"':
			inQuote = !inQuote
		case c == '}' && !inQuote:
			return i + 1, nil
		}
	}
	return 0, fmt.Errorf("unterminated label block")
}
