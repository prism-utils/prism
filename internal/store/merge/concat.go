package merge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/prism-utils/prism/internal/store/segformat"
)

// ErrNoHomogeneousPack means every schema group has fewer than two files, so
// concat would invent a rewrite of a singleton (or mix schemas).
var ErrNoHomogeneousPack = errors.New("merge: no homogeneous parquet pack")

func parquetSchemaFingerprint(path string) (string, error) {
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		return "", err
	}
	defer func() { _ = rdr.Close() }()
	sc := rdr.MetaData().Schema
	var b strings.Builder
	for i := 0; i < sc.NumColumns(); i++ {
		col := sc.Column(i)
		fmt.Fprintf(&b, "%s|%s|%s|%s|%d|%d\n",
			col.Path(),
			col.PhysicalType(),
			col.ConvertedType(),
			col.LogicalType(),
			col.MaxDefinitionLevel(),
			col.MaxRepetitionLevel(),
		)
	}
	return b.String(), nil
}

func firstHomogeneousPack(sources []Segment) ([]Segment, error) {
	ordered := sortSourcesByMinTs(sources)
	groups := map[string][]Segment{}
	var order []string
	for _, s := range ordered {
		if segformat.Payload(s.Path) == segformat.DuckDB {
			continue
		}
		fp, err := parquetSchemaFingerprint(s.Path)
		if err != nil {
			continue
		}
		if _, seen := groups[fp]; !seen {
			order = append(order, fp)
		}
		groups[fp] = append(groups[fp], s)
	}
	for _, fp := range order {
		if len(groups[fp]) >= 2 {
			return groups[fp], nil
		}
	}
	return nil, ErrNoHomogeneousPack
}

func concatParquet(dest string, sources []Segment, mem memory.Allocator) error {
	if len(sources) < 2 {
		return ErrNoHomogeneousPack
	}
	tmp := dest + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // G304: dest is a server-owned tier path
	if err != nil {
		return err
	}
	success := false
	defer func() {
		_ = out.Close()
		if !success {
			_ = os.Remove(tmp)
		}
	}()

	if mem == nil {
		mem = memory.DefaultAllocator
	}
	ctx := context.Background()
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	var writer *pqarrow.FileWriter
	var destSchema *arrow.Schema

	for _, s := range sources {
		pf, err := file.OpenParquetFile(s.Path, false)
		if err != nil {
			return err
		}
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
		if err != nil {
			_ = pf.Close()
			return err
		}
		cols := parquetColIndices(pf)
		n := pf.NumRowGroups()
		for i := 0; i < n; i++ {
			tbl, err := fr.ReadRowGroups(ctx, cols, []int{i})
			if err != nil {
				_ = pf.Close()
				if writer != nil {
					_ = writer.Close()
				}
				return err
			}
			if writer == nil {
				writer, err = pqarrow.NewFileWriter(tbl.Schema(), out, props, pqarrow.DefaultWriterProps())
				if err != nil {
					tbl.Release()
					_ = pf.Close()
					return err
				}
				destSchema = tbl.Schema()
			} else if !tbl.Schema().Equal(destSchema) {
				tbl.Release()
				_ = pf.Close()
				_ = writer.Close()
				return fmt.Errorf("merge concat: schema drifted mid-pack")
			}
			chunk := tbl.NumRows()
			if chunk <= 0 {
				chunk = 1
			}
			werr := writer.WriteTable(tbl, chunk)
			tbl.Release()
			if werr != nil {
				_ = pf.Close()
				_ = writer.Close()
				return werr
			}
		}
		_ = pf.Close()
	}
	if writer == nil {
		return fmt.Errorf("merge concat: no parquet sources")
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}
	success = true
	return nil
}

func sortSourcesByMinTs(sources []Segment) []Segment {
	out := append([]Segment(nil), sources...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].MinTs.Equal(out[j].MinTs) {
			return out[i].Path < out[j].Path
		}
		return out[i].MinTs.Before(out[j].MinTs)
	})
	return out
}

func parquetColIndices(rdr *file.Reader) []int {
	n := rdr.MetaData().Schema.NumColumns()
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}
