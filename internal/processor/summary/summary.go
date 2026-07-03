// Package summary implements a windowed group-by aggregation Processor. Given a
// RecordBatch (one buffer window), it groups rows by one or more string columns
// and emits one aggregate row per group:
//
//	group_by columns… | count | <fn>_<field>…
//
// Supported aggregates: count, sum:F, avg:F, min:F, max:F, and percentiles
// pNN:F (e.g. p95:latency, nearest-rank). Field columns must be numeric
// (int64/float64). Output rows are sorted by group key so results are
// deterministic. This is the "SQL-like summary" that feeds the JSON branch;
// the raw window still flows to the parquet branch unchanged.
package summary

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this processor.
const Type = "summary"

// Config configures the summary processor.
type Config struct {
	// GroupBy names the string columns to group by (may be empty for a single
	// global group).
	GroupBy []string `json:"group_by"`
	// Aggregates lists the aggregates to compute: "count" or "<fn>:<field>"
	// where fn is sum|avg|min|max|pNN.
	Aggregates []string `json:"aggregates"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if len(c.Aggregates) == 0 {
		return fmt.Errorf("summary.aggregates: at least one required")
	}
	for i, a := range c.Aggregates {
		if _, err := parseAggregate(a); err != nil {
			return fmt.Errorf("summary.aggregates[%d]: %w", i, err)
		}
	}
	return nil
}

type factory struct{}

// NewFactory returns the summary processor factory.
func NewFactory() component.Factory[component.Processor] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Processor, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("processor/summary: unexpected config type %T", cfg)
	}
	aggs := make([]aggregate, len(c.Aggregates))
	for i, a := range c.Aggregates {
		parsed, err := parseAggregate(a)
		if err != nil {
			return nil, fmt.Errorf("processor/summary: %w", err)
		}
		aggs[i] = parsed
	}
	return &processor{groupBy: append([]string(nil), c.GroupBy...), aggs: aggs}, nil
}

// aggregate is one parsed aggregate directive.
type aggregate struct {
	out     string  // output column name
	fn      string  // count|sum|avg|min|max|pct
	field   string  // source column (empty for count)
	pct     float64 // percentile in [0,100] when fn==pct
	isCount bool
}

func parseAggregate(a string) (aggregate, error) {
	a = strings.TrimSpace(a)
	if a == "count" {
		return aggregate{out: "count", fn: "count", isCount: true}, nil
	}
	parts := strings.SplitN(a, ":", 2)
	if len(parts) != 2 || parts[1] == "" {
		return aggregate{}, fmt.Errorf("%q: want \"count\" or \"<fn>:<field>\"", a)
	}
	fn, field := parts[0], parts[1]
	switch fn {
	case "sum", "avg", "min", "max":
		return aggregate{out: fn + "_" + field, fn: fn, field: field}, nil
	default:
		if len(fn) >= 2 && fn[0] == 'p' {
			pct, err := strconv.ParseFloat(fn[1:], 64)
			if err == nil && pct >= 0 && pct <= 100 {
				return aggregate{out: fn + "_" + field, fn: "pct", field: field, pct: pct}, nil
			}
		}
		return aggregate{}, fmt.Errorf("%q: unknown function %q", a, fn)
	}
}

type processor struct {
	groupBy []string
	aggs    []aggregate
	mem     memory.Allocator
}

func (p *processor) Start(_ context.Context, host component.Host) error {
	if host != nil {
		p.mem = host.Allocator()
	}
	if p.mem == nil {
		p.mem = memory.DefaultAllocator
	}
	return nil
}
func (p *processor) Shutdown(context.Context) error { return nil }

// group holds the running state for one group key.
type group struct {
	key  string
	vals []string             // group_by column values, in config order
	vecs map[string][]float64 // field -> collected numeric values
	n    int
}

func (p *processor) Process(_ context.Context, in data.RecordBatch) (data.RecordBatch, error) {
	defer in.Release() // processor owns its input
	rec := in.Record()
	if rec == nil {
		return p.emit(nil)
	}

	groupCols := make([]*array.String, len(p.groupBy))
	for i, name := range p.groupBy {
		col, err := stringColumn(rec, name)
		if err != nil {
			return data.RecordBatch{}, err
		}
		groupCols[i] = col
	}
	fieldCols := map[string][]float64{}
	for _, a := range p.aggs {
		if a.isCount {
			continue
		}
		if _, done := fieldCols[a.field]; done {
			continue
		}
		vec, err := numericColumn(rec, a.field)
		if err != nil {
			return data.RecordBatch{}, err
		}
		fieldCols[a.field] = vec
	}

	groups := map[string]*group{}
	var order []*group
	rows := int(rec.NumRows())
	for r := 0; r < rows; r++ {
		vals := make([]string, len(p.groupBy))
		var kb strings.Builder
		for i, col := range groupCols {
			v := col.Value(r)
			vals[i] = v
			kb.WriteString(v)
			kb.WriteByte(0)
		}
		key := kb.String()
		g := groups[key]
		if g == nil {
			g = &group{key: key, vals: vals, vecs: map[string][]float64{}}
			groups[key] = g
			order = append(order, g)
		}
		g.n++
		for field, vec := range fieldCols {
			g.vecs[field] = append(g.vecs[field], vec[r])
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i].key < order[j].key })
	return p.emit(order)
}

// emit builds the aggregate RecordBatch from the grouped state.
func (p *processor) emit(groups []*group) (data.RecordBatch, error) {
	fields := make([]arrow.Field, 0, len(p.groupBy)+len(p.aggs))
	for _, name := range p.groupBy {
		fields = append(fields, arrow.Field{Name: name, Type: arrow.BinaryTypes.String})
	}
	for _, a := range p.aggs {
		f := arrow.Field{Name: a.out, Type: arrow.PrimitiveTypes.Float64}
		if a.isCount {
			f.Type = arrow.PrimitiveTypes.Int64
		}
		fields = append(fields, f)
	}
	schema := arrow.NewSchema(fields, nil)

	groupB := make([]*array.StringBuilder, len(p.groupBy))
	for i := range groupB {
		groupB[i] = array.NewStringBuilder(p.mem)
	}
	aggB := make([]array.Builder, len(p.aggs))
	for i, a := range p.aggs {
		if a.isCount {
			aggB[i] = array.NewInt64Builder(p.mem)
		} else {
			aggB[i] = array.NewFloat64Builder(p.mem)
		}
	}

	for _, g := range groups {
		for i := range groupB {
			groupB[i].Append(g.vals[i])
		}
		for i, a := range p.aggs {
			switch {
			case a.isCount:
				aggB[i].(*array.Int64Builder).Append(int64(g.n))
			default:
				aggB[i].(*array.Float64Builder).Append(compute(a, g.vecs[a.field]))
			}
		}
	}

	cols := make([]arrow.Array, 0, len(groupB)+len(aggB))
	for _, b := range groupB {
		cols = append(cols, b.NewArray())
		b.Release()
	}
	for _, b := range aggB {
		cols = append(cols, b.NewArray())
		b.Release()
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	rec := array.NewRecordBatch(schema, cols, int64(len(groups)))
	return data.NewRecordBatch("summary", rec), nil
}

func compute(a aggregate, vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	switch a.fn {
	case "sum":
		return sum(vals)
	case "avg":
		return sum(vals) / float64(len(vals))
	case "min":
		m := vals[0]
		for _, v := range vals[1:] {
			m = math.Min(m, v)
		}
		return m
	case "max":
		m := vals[0]
		for _, v := range vals[1:] {
			m = math.Max(m, v)
		}
		return m
	case "pct":
		return percentile(vals, a.pct)
	}
	return 0
}

func sum(vals []float64) float64 {
	var s float64
	for _, v := range vals {
		s += v
	}
	return s
}

// percentile returns the nearest-rank percentile of vals.
func percentile(vals []float64, pct float64) float64 {
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	if pct <= 0 {
		return s[0]
	}
	rank := int(math.Ceil(pct / 100 * float64(len(s))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(s) {
		rank = len(s)
	}
	return s[rank-1]
}

func stringColumn(rec arrow.RecordBatch, name string) (*array.String, error) {
	idx := rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		return nil, fmt.Errorf("processor/summary: group_by column %q not found", name)
	}
	col, ok := rec.Column(idx[0]).(*array.String)
	if !ok {
		return nil, fmt.Errorf("processor/summary: group_by column %q is %s, want string", name, rec.Column(idx[0]).DataType())
	}
	return col, nil
}

// numericColumn reads a numeric column into a float64 slice (int64/float64).
func numericColumn(rec arrow.RecordBatch, name string) ([]float64, error) {
	idx := rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		return nil, fmt.Errorf("processor/summary: field %q not found", name)
	}
	rows := int(rec.NumRows())
	out := make([]float64, rows)
	switch col := rec.Column(idx[0]).(type) {
	case *array.Float64:
		for r := 0; r < rows; r++ {
			out[r] = col.Value(r)
		}
	case *array.Int64:
		for r := 0; r < rows; r++ {
			out[r] = float64(col.Value(r))
		}
	default:
		return nil, fmt.Errorf("processor/summary: field %q is %s, want int64/float64", name, col.DataType())
	}
	return out, nil
}
