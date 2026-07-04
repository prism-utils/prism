// Command prism-bench is a systematic benchmark harness for prism pipelines.
//
// It runs `prism run -config <cfg>` as a child process for a bounded window,
// then measures three things at once:
//
//  1. Execution cost — wall time, user/system CPU, and peak RSS of the child,
//     read from the OS via wait4(2) rusage (no sampling, no estimation).
//  2. Throughput — output rows produced per wall-second.
//  3. Correctness — it opens every Parquet file the run produced (reading row
//     counts and schema straight from the footer) and parses every JSON summary,
//     then reconciles those against the raw input record count so a run that
//     silently drops or duplicates records is visible as a non-zero delta.
//
// Usage:
//
//	prism-bench -config c.yaml -out /out -bin ./bin/prism \
//	    -input raw.log -duration 5s -label logs -json report.json
//
// The harness is deliberately transport-agnostic: -input is only used to count
// raw records for the reconciliation, so it works for file inputs (count lines)
// and can be omitted for scrape inputs where the raw record count is not a file.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type inputList []string

func (l *inputList) String() string { return strings.Join(*l, ",") }
func (l *inputList) Set(v string) error {
	*l = append(*l, v)
	return nil
}

// Report is the machine-readable benchmark result (also rendered as a table).
type Report struct {
	Label   string    `json:"label"`
	Config  string    `json:"config"`
	Started time.Time `json:"started"`

	// Execution cost, measured from the child process's rusage.
	WallSeconds float64 `json:"wall_seconds"`
	UserSeconds float64 `json:"user_cpu_seconds"`
	SysSeconds  float64 `json:"sys_cpu_seconds"`
	CPUPercent  float64 `json:"cpu_percent"` // (user+sys)/wall * 100
	MaxRSSMiB   float64 `json:"max_rss_mib"`

	// Correctness / throughput.
	InputRecords int64 `json:"input_records"`
	ParquetFiles int   `json:"parquet_files"`
	ParquetRows  int64 `json:"parquet_rows"`
	SummaryFiles int   `json:"summary_files"`
	SummaryGroup int   `json:"summary_groups"`
	SummaryCount int64 `json:"summary_count_total"`
	OutputBytes  int64 `json:"output_bytes"`

	// Log-template metrics, read from summary Parquet whose schema carries
	// `template` + `count`. These are the "template X → count Y" aggregates the
	// summary phase exists to produce, surfaced so a run proves them present and
	// correct — not just that bytes were written.
	TemplateGroups     int             `json:"template_groups"`
	TemplateCountTotal int64           `json:"template_count_total"`
	TopTemplates       []TemplateCount `json:"top_templates,omitempty"`

	RowsPerSecond float64 `json:"rows_per_second"`
	// RowDelta is parquet_rows - input_records; 0 means every input record was
	// preserved through parse → buffer → encode → sink.
	RowDelta int64 `json:"row_delta"`

	ParquetColumns []string `json:"parquet_columns"`
	ExitOK         bool     `json:"exit_ok"`
}

// TemplateCount is one log template and how many lines collapsed into it.
type TemplateCount struct {
	Template string `json:"template"`
	Count    int64  `json:"count"`
}

func main() {
	var (
		cfg      = flag.String("config", "", "prism config to run (required unless -inspect)")
		bin      = flag.String("bin", "prism", "path to the prism binary")
		out      = flag.String("out", "", "output root to inspect (required)")
		dur      = flag.Duration("duration", 5*time.Second, "run window before SIGINT")
		label    = flag.String("label", "bench", "scenario label")
		jsonPath = flag.String("json", "", "optional path to write the JSON report")
		inspect  = flag.Bool("inspect", false, "skip running a binary; only reconcile existing outputs (e.g. copied from a pod)")
		head     = flag.Int("head", 0, "after inspecting, print the first N rows of one Parquet file under -out as JSON (0 = off)")
		inputs   inputList
	)
	flag.Var(&inputs, "input", "raw input file to count records (repeatable; optional)")
	flag.Parse()

	if *out == "" || (*cfg == "" && !*inspect) {
		fmt.Fprintln(os.Stderr, "prism-bench: -out is required (and -config unless -inspect)")
		os.Exit(2)
	}

	var (
		rep *Report
		err error
	)
	if *inspect {
		rep, err = inspectOnly(*out, *label, inputs)
	} else {
		rep, err = run(*bin, *cfg, *out, *label, *dur, inputs)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "prism-bench:", err)
		os.Exit(1)
	}
	printTable(rep)
	if *head > 0 {
		if err := headParquet(*out, *head); err != nil {
			fmt.Fprintln(os.Stderr, "prism-bench: head:", err)
			os.Exit(1)
		}
	}
	if *jsonPath != "" {
		if err := writeJSON(*jsonPath, rep); err != nil {
			fmt.Fprintln(os.Stderr, "prism-bench: write json:", err)
			os.Exit(1)
		}
		fmt.Printf("\nJSON report: %s\n", *jsonPath)
	}
}

// inspectOnly reconciles outputs that were produced elsewhere (e.g. copied out
// of a pod): it counts input records and reads Parquet/JSON outputs, but does
// not run a process, so it reports no execution cost.
func inspectOnly(out, label string, inputs []string) (*Report, error) {
	rep := &Report{Label: label, Config: "(inspect)", Started: time.Now(), ExitOK: true}
	for _, in := range inputs {
		n, err := countLines(in)
		if err != nil {
			return nil, err
		}
		rep.InputRecords += n
	}
	if err := inspectOutputs(out, rep); err != nil {
		return nil, err
	}
	rep.RowDelta = rep.ParquetRows - rep.InputRecords
	return rep, nil
}

func run(bin, cfg, out, label string, dur time.Duration, inputs []string) (*Report, error) {
	rep := &Report{Label: label, Config: cfg, Started: time.Now()}

	// bin/cfg are operator-supplied CLI args for a local benchmark tool, not
	// untrusted input; launching the configured prism binary is the whole job.
	//nolint:gosec // G204: operator-provided paths
	cmd := exec.CommandContext(context.Background(), bin, "run", "-config", cfg)
	cmd.Stdout = os.Stderr // prism logs to stderr; keep child logs visible
	cmd.Stderr = os.Stderr
	start := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", bin, err)
	}

	// Let the pipeline run, then ask it to drain and stop cleanly.
	time.Sleep(dur)
	_ = cmd.Process.Signal(os.Interrupt)
	waitErr := cmd.Wait()
	rep.WallSeconds = time.Since(start).Seconds()
	rep.ExitOK = waitErr == nil

	if ru, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
		rep.UserSeconds = timeval(ru.Utime)
		rep.SysSeconds = timeval(ru.Stime)
		rep.MaxRSSMiB = maxRSSMiB(ru.Maxrss)
	}
	if rep.WallSeconds > 0 {
		rep.CPUPercent = (rep.UserSeconds + rep.SysSeconds) / rep.WallSeconds * 100
	}

	for _, in := range inputs {
		n, err := countLines(in)
		if err != nil {
			return nil, err
		}
		rep.InputRecords += n
	}

	if err := inspectOutputs(out, rep); err != nil {
		return nil, err
	}
	if rep.WallSeconds > 0 {
		rep.RowsPerSecond = float64(rep.ParquetRows) / rep.WallSeconds
	}
	rep.RowDelta = rep.ParquetRows - rep.InputRecords
	return rep, nil
}

// inspectOutputs walks the output tree, reading Parquet footers for row counts
// and schema, and parsing JSON summaries for group/count totals.
func inspectOutputs(root string, rep *Report) error {
	cols := map[string]struct{}{}
	templates := map[string]int64{} // template → summed count across summary files
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, ierr := d.Info()
		if ierr == nil {
			rep.OutputBytes += info.Size()
		}
		switch strings.ToLower(filepath.Ext(path)) {
		case ".parquet":
			rows, columns, perr := parquetStat(path)
			if perr != nil {
				return fmt.Errorf("parquet %s: %w", path, perr)
			}
			rep.ParquetFiles++
			rep.ParquetRows += rows
			for _, c := range columns {
				cols[c] = struct{}{}
			}
			// A Parquet summary (template + count columns) also feeds the
			// log-template metrics; raw/template phases are just row counts.
			counts, ok, terr := templateSummary(path)
			if terr != nil {
				return fmt.Errorf("template summary %s: %w", path, terr)
			}
			if ok {
				for tmpl, n := range counts {
					templates[tmpl] += n
				}
			}
		case ".json":
			groups, total, jerr := summaryStat(path)
			if jerr != nil {
				return fmt.Errorf("summary %s: %w", path, jerr)
			}
			rep.SummaryFiles++
			rep.SummaryGroup += groups
			rep.SummaryCount += total
		}
		return nil
	})
	if err != nil {
		return err
	}
	for c := range cols {
		rep.ParquetColumns = append(rep.ParquetColumns, c)
	}
	sort.Strings(rep.ParquetColumns)
	rep.setTemplateMetrics(templates)
	return nil
}

// setTemplateMetrics records the per-template aggregate on the report, sorted by
// descending count (ties broken by template text for stable output).
func (r *Report) setTemplateMetrics(templates map[string]int64) {
	for tmpl, n := range templates {
		r.TemplateGroups++
		r.TemplateCountTotal += n
		r.TopTemplates = append(r.TopTemplates, TemplateCount{Template: tmpl, Count: n})
	}
	sort.Slice(r.TopTemplates, func(i, j int) bool {
		if r.TopTemplates[i].Count != r.TopTemplates[j].Count {
			return r.TopTemplates[i].Count > r.TopTemplates[j].Count
		}
		return r.TopTemplates[i].Template < r.TopTemplates[j].Template
	})
}

// templateSummary reads a Parquet file that aggregates counts per log template:
// if its schema has both a `template` and a `count` column, it returns the
// per-template counts; otherwise ok is false (it is some other artifact). Only
// the two relevant columns are materialized.
func templateSummary(path string) (map[string]int64, bool, error) {
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rdr.Close() }()
	sc := rdr.MetaData().Schema
	hasTemplate, hasCount := false, false
	for i := 0; i < sc.NumColumns(); i++ {
		switch sc.Column(i).Name() {
		case "template":
			hasTemplate = true
		case "count":
			hasCount = true
		}
	}
	if !hasTemplate || !hasCount {
		return nil, false, nil
	}

	pr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, false, err
	}
	tbl, err := pr.ReadTable(context.Background())
	if err != nil {
		return nil, false, err
	}
	defer tbl.Release()

	tmplCol := columnByName(tbl, "template")
	cntCol := columnByName(tbl, "count")
	if tmplCol == nil || cntCol == nil {
		return nil, false, nil
	}
	counts := make(map[string]int64, tbl.NumRows())
	for r := 0; r < int(tbl.NumRows()); r++ {
		tmpl, _ := chunkedValue(tmplCol, r).(string)
		n := toInt64(chunkedValue(cntCol, r))
		counts[tmpl] += n
	}
	return counts, true, nil
}

// columnByName returns the chunked column with the given name, or nil.
func columnByName(tbl arrow.Table, name string) *arrow.Chunked {
	for i, f := range tbl.Schema().Fields() {
		if f.Name == name {
			return tbl.Column(i).Data()
		}
	}
	return nil
}

// toInt64 coerces the integer count cell regardless of its Arrow width.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// parquetStat reads row count and column names from the file footer without
// materializing any row group.
func parquetStat(path string) (int64, []string, error) {
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = rdr.Close() }()
	md := rdr.MetaData()
	sc := md.Schema
	cols := make([]string, 0, sc.NumColumns())
	for i := 0; i < sc.NumColumns(); i++ {
		cols = append(cols, sc.Column(i).Name())
	}
	return md.NumRows, cols, nil
}

// summaryStat parses a JSON array of summary rows, returning the group count and
// the sum of any integer "count" field.
func summaryStat(path string) (int, int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(b, &rows); err != nil {
		return 0, 0, err
	}
	var total int64
	for _, r := range rows {
		if v, ok := r["count"]; ok {
			if f, ok := v.(float64); ok {
				total += int64(f)
			}
		}
	}
	return len(rows), total, nil
}

// headParquet finds the first .parquet file under root and prints up to n rows
// as JSON objects, so an operator can eyeball the actual encoded content.
func headParquet(root string, n int) error {
	var target string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || target != "" {
			return err
		}
		if strings.EqualFold(filepath.Ext(path), ".parquet") {
			target = path
		}
		return nil
	})
	if err != nil {
		return err
	}
	if target == "" {
		return fmt.Errorf("no parquet file under %s", root)
	}

	rdr, err := file.OpenParquetFile(target, false)
	if err != nil {
		return err
	}
	defer func() { _ = rdr.Close() }()
	pr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	tbl, err := pr.ReadTable(context.Background())
	if err != nil {
		return err
	}
	defer tbl.Release()

	fmt.Printf("\nfirst %d row(s) of %s:\n", n, filepath.Base(target))
	fields := tbl.Schema().Fields()
	total := int(tbl.NumRows())
	if n > total {
		n = total
	}
	// Random access across chunked columns: locate the (chunk,offset) per row.
	for r := 0; r < n; r++ {
		obj := make(map[string]any, len(fields))
		for c := range fields {
			col := tbl.Column(c).Data()
			obj[fields[c].Name] = chunkedValue(col, r)
		}
		b, _ := json.Marshal(obj)
		fmt.Printf("  %s\n", b)
	}
	return nil
}

// chunkedValue resolves logical row r within a chunked column to its cell value.
func chunkedValue(col *arrow.Chunked, r int) any {
	for _, chunk := range col.Chunks() {
		if r < chunk.Len() {
			return value(chunk, r)
		}
		r -= chunk.Len()
	}
	return nil
}

// value extracts one cell as a Go value for JSON display.
func value(col arrow.Array, r int) any {
	if col.IsNull(r) {
		return nil
	}
	switch a := col.(type) {
	case *array.String:
		return a.Value(r)
	case *array.Binary:
		return string(a.Value(r))
	case *array.Boolean:
		return a.Value(r)
	case *array.Int64:
		return a.Value(r)
	case *array.Int32:
		return a.Value(r)
	case *array.Float64:
		f := a.Value(r)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Sprintf("%f", f) // display-only; keeps JSON valid
		}
		return f
	default:
		return fmt.Sprintf("<%s>", col.DataType())
	}
}

func countLines(path string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("count %s: %w", path, err)
	}
	if len(b) == 0 {
		return 0, nil
	}
	n := int64(0)
	for _, c := range b {
		if c == '\n' {
			n++
		}
	}
	if b[len(b)-1] != '\n' { // last line without trailing newline
		n++
	}
	return n, nil
}

func timeval(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}

// maxRSSMiB normalizes ru_maxrss, whose unit differs by OS: bytes on Darwin,
// kilobytes on Linux.
func maxRSSMiB(maxrss int64) float64 {
	switch runtime.GOOS {
	case "darwin":
		return float64(maxrss) / (1024 * 1024)
	default: // linux and other unices report kilobytes
		return float64(maxrss) / 1024
	}
}

func writeJSON(path string, rep *Report) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

func printTable(r *Report) {
	line := strings.Repeat("─", 52)
	fmt.Printf("\n%s\n  prism-bench: %s\n%s\n", line, r.Label, line)
	rows := [][2]string{
		{"config", r.Config},
		{"exit ok", fmt.Sprintf("%v", r.ExitOK)},
	}
	if r.Config != "(inspect)" { // execution cost is meaningless in inspect mode
		rows = append(rows,
			[2]string{"", ""},
			[2]string{"wall time", fmt.Sprintf("%.3f s", r.WallSeconds)},
			[2]string{"user CPU", fmt.Sprintf("%.3f s", r.UserSeconds)},
			[2]string{"sys CPU", fmt.Sprintf("%.3f s", r.SysSeconds)},
			[2]string{"CPU utilization", fmt.Sprintf("%.1f %%", r.CPUPercent)},
			[2]string{"peak RSS", fmt.Sprintf("%.1f MiB", r.MaxRSSMiB)},
			[2]string{"throughput", fmt.Sprintf("%.0f rows/s", r.RowsPerSecond)},
		)
	}
	rows = append(rows, [][2]string{
		{"", ""},
		{"input records", fmt.Sprintf("%d", r.InputRecords)},
		{"parquet files", fmt.Sprintf("%d", r.ParquetFiles)},
		{"parquet rows", fmt.Sprintf("%d", r.ParquetRows)},
		{"row delta (out-in)", fmt.Sprintf("%+d", r.RowDelta)},
		{"summary files", fmt.Sprintf("%d", r.SummaryFiles)},
		{"summary groups", fmt.Sprintf("%d", r.SummaryGroup)},
		{"summary count sum", fmt.Sprintf("%d", r.SummaryCount)},
		{"output bytes", fmt.Sprintf("%d", r.OutputBytes)},
		{"parquet columns", strings.Join(r.ParquetColumns, ", ")},
	}...)
	if r.TemplateGroups > 0 {
		rows = append(rows,
			[2]string{"", ""},
			[2]string{"log templates", fmt.Sprintf("%d", r.TemplateGroups)},
			[2]string{"templated lines", fmt.Sprintf("%d", r.TemplateCountTotal)},
		)
	}
	for _, kv := range rows {
		if kv[0] == "" {
			fmt.Println()
			continue
		}
		fmt.Printf("  %-18s %s\n", kv[0], kv[1])
	}
	if r.TemplateGroups > 0 {
		printTopTemplates(r.TopTemplates)
	}
	fmt.Println(line)
}

// printTopTemplates renders the highest-count log templates ("template X →
// count Y"), the chartable aggregate the summary phase produces.
func printTopTemplates(top []TemplateCount) {
	const max = 10
	n := len(top)
	if n > max {
		n = max
	}
	fmt.Printf("\n  top %d log templates (count → template):\n", n)
	for _, tc := range top[:n] {
		tmpl := tc.Template
		if len(tmpl) > 60 {
			tmpl = tmpl[:57] + "..."
		}
		fmt.Printf("    %8d  %s\n", tc.Count, tmpl)
	}
}
