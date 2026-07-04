// Package collect implements a minimal Apache Arrow Flight receiver: a Flight
// server whose DoPut handler persists each received window to a Parquet file,
// reusing the same time-range naming as the local dir output so artifacts are
// range-selectable regardless of how they arrived. It is the server side that
// makes the flight output testable end to end, and a usable ingest endpoint.
package collect

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	outputdir "github.com/elk-utilities/prism/internal/output/dir"
)

// Server receives Flight DoPut streams and writes them to Parquet under Dir.
type Server struct {
	flight.BaseFlightServer
	dir *outputdir.Output
	log *slog.Logger
	mem memory.Allocator
}

// NewServer builds a receiver that persists windows under dir.
func NewServer(dir string, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}
	out, err := outputdir.NewFactory().Create(&outputdir.Config{Dir: dir}, component.Settings{Logger: log})
	if err != nil {
		return nil, fmt.Errorf("collect: build dir output: %w", err)
	}
	o, ok := out.(*outputdir.Output)
	if !ok {
		return nil, fmt.Errorf("collect: unexpected output type %T", out)
	}
	if err := o.Start(context.Background(), nil); err != nil {
		return nil, fmt.Errorf("collect: start dir output: %w", err)
	}
	return &Server{dir: o, log: log, mem: memory.DefaultAllocator}, nil
}

// Serve binds a Flight server to addr and blocks until ctx is cancelled. It
// reports the bound address via the ready callback (useful when addr uses :0).
func (s *Server) Serve(ctx context.Context, addr string, ready func(bound string)) error {
	srv := flight.NewServerWithMiddleware(nil)
	if err := srv.Init(addr); err != nil {
		return fmt.Errorf("collect: init %q: %w", addr, err)
	}
	srv.RegisterFlightService(s)
	if ready != nil {
		ready(srv.Addr().String())
	}
	go func() {
		<-ctx.Done()
		srv.Shutdown()
	}()
	if err := srv.Serve(); err != nil {
		return fmt.Errorf("collect: serve: %w", err)
	}
	return nil
}

// DoPut reads an incoming record stream and writes it to one Parquet file named
// from the descriptor path [pipeline, branch, startNano, endNano].
func (s *Server) DoPut(stream flight.FlightService_DoPutServer) error {
	rdr, err := flight.NewRecordReader(stream, ipc.WithAllocator(s.mem))
	if err != nil {
		return fmt.Errorf("collect: reader: %w", err)
	}
	defer rdr.Release()
	meta := metaFromDescriptor(rdr.LatestFlightDescriptor())

	var recs []arrow.RecordBatch
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		recs = append(recs, rec)
	}
	if err := rdr.Err(); err != nil {
		releaseAll(recs)
		return fmt.Errorf("collect: read: %w", err)
	}
	defer releaseAll(recs)

	if len(recs) > 0 {
		block, err := toParquet(recs, &meta)
		if err != nil {
			return err
		}
		if err := s.dir.Consume(stream.Context(), block); err != nil {
			return fmt.Errorf("collect: persist: %w", err)
		}
		s.log.Info("collect: wrote window",
			"pipeline", meta.Pipeline, "branch", meta.Branch, "rows", block.Rows)
	}
	return stream.Send(&flight.PutResult{})
}

// toParquet serializes the received records into one Parquet block, stamped with
// the provenance the dir output needs to name the file.
func toParquet(recs []arrow.RecordBatch, meta *data.BlockMeta) (data.EncodedBlock, error) {
	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	fw, err := pqarrow.NewFileWriter(recs[0].Schema(), &buf, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return data.EncodedBlock{}, fmt.Errorf("collect: parquet writer: %w", err)
	}
	rows := 0
	for _, rec := range recs {
		if err := fw.Write(rec); err != nil {
			_ = fw.Close()
			return data.EncodedBlock{}, fmt.Errorf("collect: parquet write: %w", err)
		}
		rows += int(rec.NumRows())
	}
	if err := fw.Close(); err != nil {
		return data.EncodedBlock{}, fmt.Errorf("collect: parquet close: %w", err)
	}
	return data.EncodedBlock{Format: "parquet", Bytes: buf.Bytes(), Rows: rows, Meta: meta}, nil
}

// metaFromDescriptor decodes the provenance the flight output encoded into the
// descriptor path [pipeline, branch, startNano, endNano].
func metaFromDescriptor(d *flight.FlightDescriptor) data.BlockMeta {
	m := data.BlockMeta{Pipeline: "flight", Branch: "raw"}
	if d == nil || len(d.Path) < 4 {
		return m
	}
	m.Pipeline, m.Branch = d.Path[0], d.Path[1]
	m.Window.Start = timeFromNano(d.Path[2])
	m.Window.End = timeFromNano(d.Path[3])
	return m
}

func timeFromNano(s string) time.Time {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func releaseAll(recs []arrow.RecordBatch) {
	for _, r := range recs {
		r.Release()
	}
}
