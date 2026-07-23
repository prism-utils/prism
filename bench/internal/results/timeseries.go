package results

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/elk-utilities/prism/bench/internal/monitor"
)

const maxTimeseriesPoints = 3000

// TimeseriesSeries is one system's dense sample stream.
type TimeseriesSeries struct {
	System string                `json:"system"`
	Target string                `json:"target"`
	Points []monitor.SamplePoint `json:"points"`
}

// TimeseriesReport is persisted to bench/results-timeseries.json.
type TimeseriesReport struct {
	Phases     []monitor.PhaseSpan   `json:"phases"`
	Series     []TimeseriesSeries    `json:"series"`
	Store      []monitor.SamplePoint `json:"store_stitched,omitempty"`
	ClickHouse []monitor.SamplePoint `json:"clickhouse,omitempty"`
}

// WriteTimeseriesJSON writes the downsampled time series report.
func WriteTimeseriesJSON(path string, rep *TimeseriesReport) error {
	out := *rep
	out.Store = monitor.Downsample(rep.Store, maxTimeseriesPoints)
	out.ClickHouse = monitor.Downsample(rep.ClickHouse, maxTimeseriesPoints)
	for i := range out.Series {
		out.Series[i].Points = monitor.Downsample(out.Series[i].Points, maxTimeseriesPoints)
	}
	b, err := json.MarshalIndent(&out, "", "  ")
	if err != nil {
		return fmt.Errorf("results: marshal timeseries: %w", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		return fmt.Errorf("results: write timeseries: %w", err)
	}
	return nil
}
