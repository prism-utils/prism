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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
	"github.com/prism-utils/prism/internal/encoder/bloom"
)

// Type is the config identifier for this encoder.
const Type = "parquet"

// BloomConfig configures optional footer KV substring bloom indexes.
type BloomConfig struct {
	Enabled bool     `json:"enabled"`
	Columns []string `json:"columns"`
	Tokens  bool     `json:"tokens"`
	Ngram   int      `json:"ngram"`
	FP      float64  `json:"fp"`
}

// Config configures the parquet encoder.
type Config struct {
	// Compression is one of snappy (default), zstd, gzip, none.
	Compression string `json:"compression"`
	// RowGroupRows splits the window into row-groups of this many rows; 0 keeps
	// one row-group for the entire window.
	RowGroupRows int         `json:"row_group_rows"`
	Bloom        BloomConfig `json:"bloom"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if _, err := codec(c.Compression); err != nil {
		return err
	}
	if c.RowGroupRows < 0 {
		return fmt.Errorf("parquet.row_group_rows: must be >= 0")
	}
	if c.Bloom.Enabled {
		if c.Bloom.FP <= 0 || c.Bloom.FP >= 1 {
			return fmt.Errorf("parquet.bloom.fp: must be in (0,1)")
		}
		if c.Bloom.Ngram != 0 && c.Bloom.Ngram < 2 {
			return fmt.Errorf("parquet.bloom.ngram: must be 0 or >= 2")
		}
		if len(c.Bloom.Columns) == 0 {
			return fmt.Errorf("parquet.bloom.columns: must be non-empty when bloom is enabled")
		}
	} else if c.Bloom.FP != 0 && (c.Bloom.FP <= 0 || c.Bloom.FP >= 1) {
		return fmt.Errorf("parquet.bloom.fp: must be in (0,1)")
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

func (factory) Type() string { return Type }

func (factory) DefaultConfig() component.Config {
	return &Config{
		Compression:  "snappy",
		RowGroupRows: 0,
		Bloom: BloomConfig{
			Enabled: true,
			Columns: []string{"message"},
			Tokens:  true,
			Ngram:   3,
			FP:      0.01,
		},
	}
}

func (factory) Create(cfg component.Config, _ component.Settings) (component.Encoder, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("encoder/parquet: unexpected config type %T", cfg)
	}
	cc, err := codec(c.Compression)
	if err != nil {
		return nil, err
	}
	return &encoder{
		codec:        cc,
		cfg:          *c,
		hashSet:      make(map[uint64]struct{}, 256),
		lowerScratch: make([]byte, 0, 4096),
		runeOffsets:  make([]int, 0, 256),
	}, nil
}

type encoder struct {
	codec        compress.Compression
	cfg          Config
	hashSet      map[uint64]struct{}
	lowerScratch []byte
	runeOffsets  []int
}

func (*encoder) Start(context.Context, component.Host) error { return nil }
func (*encoder) Shutdown(context.Context) error              { return nil }

type bloomKV struct {
	key   string
	value string
}

func (e *encoder) Encode(_ context.Context, in data.RecordBatch) (data.EncodedBlock, error) {
	defer in.Release()
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

	ranges := rowGroupRanges(int(rec.NumRows()), e.cfg.RowGroupRows)
	var footerKV []bloomKV
	indexedCols := make(map[string]struct{})

	for rg, rgRange := range ranges {
		slice := rec.NewSlice(rgRange.start, rgRange.end)
		if err := fw.Write(slice); err != nil {
			slice.Release()
			_ = fw.Close()
			return data.EncodedBlock{}, fmt.Errorf("encoder/parquet: write row-group %d: %w", rg, err)
		}
		if e.cfg.Bloom.Enabled {
			kv, cols, err := e.buildBloomKV(slice, rg)
			if err != nil {
				slice.Release()
				_ = fw.Close()
				return data.EncodedBlock{}, err
			}
			footerKV = append(footerKV, kv...)
			for c := range cols {
				indexedCols[c] = struct{}{}
			}
		}
		slice.Release()
	}

	for col := range indexedCols {
		params, err := bloom.MarshalParams(bloom.ParamsWithNgram(e.cfg.Bloom.FP, e.cfg.Bloom.Ngram))
		if err != nil {
			_ = fw.Close()
			return data.EncodedBlock{}, fmt.Errorf("encoder/parquet: bloom params: %w", err)
		}
		footerKV = append(footerKV, bloomKV{
			key:   fmt.Sprintf("prism.bloom.v1.%s.params", col),
			value: params,
		})
	}

	for _, kv := range footerKV {
		if err := fw.AppendKeyValueMetadata(kv.key, kv.value); err != nil {
			_ = fw.Close()
			return data.EncodedBlock{}, fmt.Errorf("encoder/parquet: footer kv: %w", err)
		}
	}

	if err := fw.Close(); err != nil {
		return data.EncodedBlock{}, fmt.Errorf("encoder/parquet: close: %w", err)
	}
	return data.EncodedBlock{Format: Type, Bytes: buf.Bytes(), Rows: int(rec.NumRows())}, nil
}

type rowRange struct {
	start, end int64
}

func rowGroupRanges(total int, chunk int) []rowRange {
	if chunk <= 0 {
		return []rowRange{{start: 0, end: int64(total)}}
	}
	var out []rowRange
	for start := 0; start < total; start += chunk {
		end := start + chunk
		if end > total {
			end = total
		}
		out = append(out, rowRange{start: int64(start), end: int64(end)})
	}
	return out
}

func (e *encoder) buildBloomKV(rec arrow.RecordBatch, rg int) ([]bloomKV, map[string]struct{}, error) {
	var out []bloomKV
	indexed := make(map[string]struct{})
	for _, colName := range e.cfg.Bloom.Columns {
		idx := rec.Schema().FieldIndices(colName)
		if len(idx) == 0 {
			continue
		}
		strCol, ok := rec.Column(idx[0]).(*array.String)
		if !ok {
			continue
		}
		indexed[colName] = struct{}{}
		data := strCol.ValueBytes()

		if e.cfg.Bloom.Tokens {
			clear(e.hashSet)
			for i := 0; i < strCol.Len(); i++ {
				if strCol.IsNull(i) {
					continue
				}
				bloom.AddWordHashesBytes(e.hashSet, stringRowBytes(strCol, data, i))
			}
			f := bloom.BuildFromHashes(e.hashSet, e.cfg.Bloom.FP)
			blob, err := f.Marshal()
			if err != nil {
				return nil, nil, fmt.Errorf("encoder/parquet: bloom marshal: %w", err)
			}
			out = append(out, bloomKV{
				key:   fmt.Sprintf("prism.bloom.v1.%s.tokens.rg%d", colName, rg),
				value: blob,
			})
		}
		if e.cfg.Bloom.Ngram >= 2 {
			n := e.cfg.Bloom.Ngram
			clear(e.hashSet)
			for i := 0; i < strCol.Len(); i++ {
				if strCol.IsNull(i) {
					continue
				}
				row := stringRowBytes(strCol, data, i)
				e.runeOffsets, e.lowerScratch = bloom.AddTrigramHashesBytes(
					e.hashSet, e.lowerScratch, row, e.runeOffsets, n)
			}
			f := bloom.BuildFromHashes(e.hashSet, e.cfg.Bloom.FP)
			blob, err := f.Marshal()
			if err != nil {
				return nil, nil, fmt.Errorf("encoder/parquet: bloom marshal: %w", err)
			}
			out = append(out, bloomKV{
				key:   fmt.Sprintf("prism.bloom.v1.%s.ngram.rg%d", colName, rg),
				value: blob,
			})
		}
	}
	return out, indexed, nil
}

// stringRowBytes returns row i as UTF-8 bytes from the column's contiguous value buffer.
func stringRowBytes(col *array.String, data []byte, i int) []byte {
	offs := col.ValueOffsets()
	base := int(offs[0])
	start := int(offs[i]) - base
	end := int(offs[i+1]) - base
	return data[start:end]
}
