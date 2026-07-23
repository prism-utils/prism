//go:build duckdb_arrow

package query

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

func writeArrowResponse(ctx context.Context, w http.ResponseWriter, conn *sql.Conn, userSQL string, rowCap int, logger *slog.Logger) error {
	wrapped := fmt.Sprintf("SELECT * FROM (%s) AS _prism_q LIMIT %d", userSQL, rowCap+1)

	var reader array.RecordReader
	err := conn.Raw(func(dc any) error {
		driverConn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("query: raw conn: %w", errSandboxExec)
		}
		ar, err := duckdb.NewArrowFromConn(driverConn)
		if err != nil {
			return wrapSandboxErr(fmt.Errorf("arrow from conn: %w", err))
		}
		rr, err := ar.QueryContext(ctx, wrapped)
		if err != nil {
			return wrapSandboxErr(err)
		}
		reader = rr
		return nil
	})
	if err != nil {
		return err
	}
	defer reader.Release()

	w.Header().Set("Content-Type", arrowStreamMediaType)
	w.Header().Set("Trailer", truncatedTrailer)
	w.WriteHeader(http.StatusOK)

	ipcWriter := ipc.NewWriter(w, ipc.WithSchema(reader.Schema()))

	flusher, _ := w.(http.Flusher)

	var totalRows int
	truncated := false

	for reader.Next() {
		rec := reader.RecordBatch()
		batchRows := int(rec.NumRows())
		if batchRows == 0 {
			continue
		}
		if totalRows >= rowCap {
			truncated = true
			break
		}

		if totalRows+batchRows <= rowCap {
			if err := ipcWriter.Write(rec); err != nil {
				logger.Error("sql arrow write", "err", err)
				return nil
			}
			totalRows += batchRows
			flushResponse(flusher)
			continue
		}

		if totalRows < rowCap {
			keep := rowCap - totalRows
			sliced := rec.NewSlice(0, int64(keep))
			if err := ipcWriter.Write(sliced); err != nil {
				sliced.Release()
				logger.Error("sql arrow write slice", "err", err)
				return nil
			}
			sliced.Release()
			totalRows = rowCap
		}
		truncated = true
		break
	}

	if err := reader.Err(); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			logger.Error("sql arrow stream", "err", err)
		}
		return nil
	}

	if totalRows > rowCap {
		truncated = true
	}

	if err := ipcWriter.Close(); err != nil {
		logger.Error("sql arrow close", "err", err)
		return nil
	}

	w.Header().Set(truncatedTrailer, strconv.FormatBool(truncated))
	flushResponse(flusher)
	return nil
}

func flushResponse(flusher http.Flusher) {
	if flusher != nil {
		flusher.Flush()
	}
}
