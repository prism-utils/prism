package results

import (
	"fmt"
	"image/color"
	"os"
	"time"

	"github.com/elk-utilities/prism/bench/internal/monitor"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

const chartWidth = 10 * vg.Inch
const chartHeight = 4 * vg.Inch

// WriteCPUChart renders CPU cores over time for both systems.
func WriteCPUChart(path string, store, clickhouse []monitor.SamplePoint, phases []monitor.PhaseSpan) error {
	return writeTimelineChart(path, "CPU cores", "cores", store, clickhouse, phases, func(p monitor.SamplePoint) float64 {
		return p.CPUCores
	})
}

// WriteMemoryChart renders peak RSS in MiB over time for both systems.
func WriteMemoryChart(path string, store, clickhouse []monitor.SamplePoint, phases []monitor.PhaseSpan) error {
	return writeTimelineChart(path, "Memory RSS (MiB)", "MiB", store, clickhouse, phases, func(p monitor.SamplePoint) float64 {
		return float64(p.RSSBytes) / (1024 * 1024)
	})
}

// WriteIOChart renders combined read+write MiB/s when any I/O data exists.
func WriteIOChart(path string, store, clickhouse []monitor.SamplePoint, phases []monitor.PhaseSpan) (bool, error) {
	if !seriesHasIO(store) && !seriesHasIO(clickhouse) {
		return false, nil
	}
	err := writeTimelineChart(path, "Disk I/O (MiB/sample interval)", "MiB", store, clickhouse, phases, func(p monitor.SamplePoint) float64 {
		return float64(p.ReadB+p.WriteB) / (1024 * 1024)
	})
	return err == nil, err
}

func seriesHasIO(points []monitor.SamplePoint) bool {
	for _, p := range points {
		if p.ReadB > 0 || p.WriteB > 0 || p.ReadOps > 0 || p.WriteOps > 0 {
			return true
		}
	}
	return false
}

func writeTimelineChart(path, title, yLabel string, store, clickhouse []monitor.SamplePoint, phases []monitor.PhaseSpan, y func(monitor.SamplePoint) float64) error {
	if len(store) == 0 && len(clickhouse) == 0 {
		return fmt.Errorf("results: chart %s: no data", title)
	}
	t0 := chartOrigin(store, clickhouse, phases)
	p := plot.New()
	p.Title.Text = title
	p.X.Label.Text = "seconds since run start"
	p.Y.Label.Text = yLabel
	p.Legend.Top = true

	for _, ph := range phases {
		if ph.End.Before(ph.Start) {
			continue
		}
		addPhaseBand(p, t0, ph)
	}

	if pts, ok := toXY(store, t0, y); ok {
		l, err := plotter.NewLine(pts)
		if err != nil {
			return fmt.Errorf("results: store line: %w", err)
		}
		l.Color = color.RGBA{R: 0x22, G: 0x88, B: 0x55, A: 0xff}
		l.Width = vg.Points(1.5)
		p.Add(l)
		p.Legend.Add("prism-store", l)
	}
	if pts, ok := toXY(clickhouse, t0, y); ok {
		l, err := plotter.NewLine(pts)
		if err != nil {
			return fmt.Errorf("results: clickhouse line: %w", err)
		}
		l.Color = color.RGBA{R: 0xff, G: 0x66, B: 0x00, A: 0xff}
		l.Width = vg.Points(1.5)
		p.Add(l)
		p.Legend.Add("ClickHouse", l)
	}

	w, err := p.WriterTo(chartWidth, chartHeight, "svg")
	if err != nil {
		return fmt.Errorf("results: chart writer: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("results: create chart: %w", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := w.WriteTo(f); err != nil {
		return fmt.Errorf("results: write svg: %w", err)
	}
	return nil
}

func chartOrigin(store, clickhouse []monitor.SamplePoint, phases []monitor.PhaseSpan) time.Time {
	var t0 time.Time
	for _, p := range store {
		if t0.IsZero() || p.At.Before(t0) {
			t0 = p.At
		}
	}
	for _, p := range clickhouse {
		if t0.IsZero() || p.At.Before(t0) {
			t0 = p.At
		}
	}
	for _, ph := range phases {
		if t0.IsZero() || ph.Start.Before(t0) {
			t0 = ph.Start
		}
	}
	return t0
}

func toXY(points []monitor.SamplePoint, t0 time.Time, y func(monitor.SamplePoint) float64) (plotter.XYs, bool) {
	if len(points) == 0 {
		return nil, false
	}
	out := make(plotter.XYs, len(points))
	for i, p := range points {
		out[i].X = p.At.Sub(t0).Seconds()
		out[i].Y = y(p)
	}
	return out, true
}

func addPhaseBand(p *plot.Plot, t0 time.Time, ph monitor.PhaseSpan) {
	x0 := ph.Start.Sub(t0).Seconds()
	x1 := ph.End.Sub(t0).Seconds()
	if x1 <= x0 {
		return
	}
	rect, err := plotter.NewPolygon(plotter.XYs{{X: x0, Y: p.Y.Min}, {X: x1, Y: p.Y.Min}, {X: x1, Y: p.Y.Max}, {X: x0, Y: p.Y.Max}})
	if err != nil {
		return
	}
	fill := phaseFill(ph.Name)
	rect.Color = fill
	rect.Width = vg.Length(0)
	p.Add(rect)
}

func phaseFill(name string) color.Color {
	switch name {
	case monitor.PhaseIdle:
		return color.RGBA{R: 0xdd, G: 0xdd, B: 0xdd, A: 0x55}
	case monitor.PhaseIngest:
		return color.RGBA{R: 0xaa, G: 0xcc, B: 0xff, A: 0x44}
	case monitor.PhaseCount:
		return color.RGBA{R: 0xff, G: 0xee, B: 0xaa, A: 0x44}
	case monitor.PhaseAggregation:
		return color.RGBA{R: 0xcc, G: 0xff, B: 0xcc, A: 0x44}
	default:
		return color.RGBA{R: 0xee, G: 0xee, B: 0xff, A: 0x33}
	}
}
