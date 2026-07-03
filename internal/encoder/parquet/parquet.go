// Package parquet implements an Encoder that serializes an Arrow RecordBatch
// into a complete, self-contained Parquet file (one file per buffer window).
// It uses arrow-go's pure-Go pqarrow writer, so it needs no CGO and keeps the
// single-static-binary guarantee. It is the durable "raw data" sink; the JSON
// summary rides a separate branch.
package parquet

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this encoder.
const Type = "parquet"

// Config configures the parquet encoder.
type Config struct {
	// Compression is one of snappy (default), zstd, gzip, none.
	Compression string `json:"compression"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if _, err := codec(c.Compression); err != nil {
		return err
	}
	return nil
}

func codec(name string) (compress.Compression, error) {
	switch name {
	case "", "snappy":
		return compress.Codecs.Snappy, nil
	case "zstd":
		return compress.Codecs.Zstd, nil
	case "gzip":
		return compress.Codecs.Gzip, nil
	case "none", "uncompressed":
		return compress.Codecs.Uncompressed, nil
	default:
		return 0, fmt.Errorf("parquet.compression: unknown codec %q (want snappy|zstd|gzip|none)", name)
	}
}

type factory struct{}

// NewFactory returns the parquet encoder factory.
func NewFactory() component.Factory[component.Encoder] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{Compression: "snappy"} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Encoder, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("encoder/parquet: unexpected config type %T", cfg)
	}
	cc, err := codec(c.Compression)
	if err != nil {
		return nil, err
	}
	return &encoder{codec: cc}, nil
}

type encoder struct{ codec compress.Compression }

func (*encoder) Start(context.Context, component.Host) error { return nil }
func (*encoder) Shutdown(context.Context) error              { return nil }

func (e *encoder) Encode(_ context.Context, in data.RecordBatch) (data.EncodedBlock, error) {
	defer in.Release() // encoders own their input's buffers
	rec := in.Record()
	if rec == nil || rec.NumRows() == 0 {
		return data.EncodedBlock{Format: Type, Rows: 0}, nil
	}

	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithCompression(e.codec))
	fw, err := pqarrow.NewFileWriter(rec.Schema(), &buf, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return data.EncodedBlock{}, fmt.Errorf("encoder/parquet: new writer: %w", err)
	}
	if err := fw.Write(rec); err != nil {
		_ = fw.Close()
		return data.EncodedBlock{}, fmt.Errorf("encoder/parquet: write: %w", err)
	}
	if err := fw.Close(); err != nil {
		return data.EncodedBlock{}, fmt.Errorf("encoder/parquet: close: %w", err)
	}
	return data.EncodedBlock{Format: Type, Bytes: buf.Bytes(), Rows: int(rec.NumRows())}, nil
}
