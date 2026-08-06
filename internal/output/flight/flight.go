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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/duckdbfile"
	"github.com/elk-utilities/prism/internal/tlsconf"
)

// Type is the config identifier for this output.
const Type = "flight"

// Config configures the flight output.
type Config struct {
	// Addr is the Flight server endpoint (host:port). Required.
	Addr string `json:"addr"`
	// Token, when set, is sent as "authorization: Bearer <token>" per-RPC so
	// the window reaches a Bearer-checking ingress. Use ${ENV}.
	Token string `json:"token,omitempty"`
	// TLS enables transport security for the gRPC connection. When unset the
	// connection is insecure (plaintext) — acceptable only on a trusted network.
	TLS *tlsconf.Config `json:"tls,omitempty"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("flight.addr: required, must not be empty")
	}
	if err := c.TLS.Validate("flight.tls"); err != nil {
		return fmt.Errorf("%w", err)
	}
	return nil
}

// bearerToken attaches "authorization: Bearer <token>" to every RPC. It only
// permits an insecure transport when TLS is not configured, so a token over
// plaintext is a deliberate trusted-network choice, never an accident.
type bearerToken struct {
	token      string
	requireTLS bool
}

func (b bearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearerToken) RequireTransportSecurity() bool { return b.requireTLS }

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

// Start dials the Flight server, applying TLS transport credentials and a
// per-RPC bearer token when configured (plaintext otherwise).
func (o *Output) Start(_ context.Context, _ component.Host) error {
	var dialOpts []grpc.DialOption
	if o.cfg.TLS != nil {
		tlsCfg, err := o.cfg.TLS.Build()
		if err != nil {
			return fmt.Errorf("output/flight: tls: %w", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if o.cfg.Token != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(bearerToken{token: o.cfg.Token, requireTLS: o.cfg.TLS != nil}))
	}
	c, err := flight.NewClientWithMiddleware(o.cfg.Addr, nil, nil, dialOpts...)
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

// descriptorPathDuckDB appends format=duckdb so the receiver can branch without
// parsing the payload as Arrow IPC.
func descriptorPathDuckDB(m *data.BlockMeta) []string {
	return append(descriptorPath(m), duckdbfile.FormatMeta)
}
