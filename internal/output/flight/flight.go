// Package flight implements an Output that ships a window to an Apache Arrow
// Flight server via DoPut. The block carries an Arrow IPC stream (from the
// `arrow` encoder); this output reframes those records as FlightData so the
// receiver ingests the columns directly into columnar storage — no row-by-row
// re-parse on the server. The producing pipeline/branch/window ride in the
// FlightDescriptor path so the receiver can name artifacts consistently.
package flight

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this output.
const Type = "flight"

// Config configures the flight output.
type Config struct {
	// Addr is the Flight server endpoint (host:port). Required.
	Addr string `json:"addr"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("flight.addr: required, must not be empty")
	}
	return nil
}

type factory struct{}

// NewFactory returns the flight output factory.
func NewFactory() component.Factory[component.Output] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Output, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("output/flight: unexpected config type %T", cfg)
	}
	return &Output{cfg: *c}, nil
}

// Output streams encoded windows to a Flight server.
type Output struct {
	cfg    Config
	client flight.Client
}

// Start dials the Flight server (insecure transport for this cut).
func (o *Output) Start(_ context.Context, _ component.Host) error {
	c, err := flight.NewClientWithMiddleware(o.cfg.Addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("output/flight: dial %q: %w", o.cfg.Addr, err)
	}
	o.client = c
	return nil
}

// Shutdown closes the Flight client connection.
func (o *Output) Shutdown(context.Context) error {
	if o.client != nil {
		return o.client.Close()
	}
	return nil
}

// Consume reads the block's IPC records and DoPuts them to the server.
func (o *Output) Consume(ctx context.Context, block data.EncodedBlock) error {
	if len(block.Bytes) == 0 {
		return nil // empty window
	}
	rdr, err := ipc.NewReader(bytes.NewReader(block.Bytes))
	if err != nil {
		return fmt.Errorf("output/flight: ipc reader: %w", err)
	}
	defer rdr.Release()

	stream, err := o.client.DoPut(ctx)
	if err != nil {
		return fmt.Errorf("output/flight: doput: %w", err)
	}
	w := flight.NewRecordWriter(stream, ipc.WithSchema(rdr.Schema()))
	w.SetFlightDescriptor(&flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: descriptorPath(block.Meta)})
	for rdr.Next() {
		if err := w.Write(rdr.RecordBatch()); err != nil {
			_ = w.Close()
			return fmt.Errorf("output/flight: write: %w", err)
		}
	}
	if err := rdr.Err(); err != nil {
		_ = w.Close()
		return fmt.Errorf("output/flight: read: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("output/flight: close writer: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("output/flight: close send: %w", err)
	}
	// Drain server acknowledgements until the stream ends.
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("output/flight: ack: %w", err)
		}
	}
}

// descriptorPath encodes the artifact provenance as the Flight descriptor path:
// [pipeline, branch, startUnixNano, endUnixNano]. The receiver decodes it to
// name the persisted artifact. Zero/absent values degrade to "unknown"/"0".
func descriptorPath(m *data.BlockMeta) []string {
	if m == nil {
		return []string{"unknown", "unknown", "0", "0"}
	}
	return []string{
		orUnknown(m.Pipeline),
		orUnknown(m.Branch),
		nano(m.Window.Start),
		nano(m.Window.End),
	}
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

// nano encodes a time as its Unix-nanosecond string, or "0" for the zero time
// (whose UnixNano would otherwise be a meaningless large negative number).
func nano(t time.Time) string {
	if t.IsZero() {
		return "0"
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}
