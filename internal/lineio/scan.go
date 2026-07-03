// Package lineio holds small, dependency-free helpers shared by the file-based
// inputs. It is a leaf utility (not a component), so inputs may import it
// without any component importing another component (docs/CONTRIBUTING.md §3.1).
package lineio

import (
	"bufio"
	"context"
	"io"

	"github.com/elk-utilities/prism/internal/data"
)

// MaxLineBytes caps a single scanned line, protecting memory on pathological
// input (a file with no newlines).
const MaxLineBytes = 1 << 20 // 1 MiB

// DefaultInitLineBytes is the scanner's starting buffer size.
const DefaultInitLineBytes = 64 << 10

// ScanLines reads newline-delimited records from r and sends bounded RawBatches
// (up to batchSize records each) to out. It copies every line because
// bufio.Scanner reuses its buffer, and honors ctx cancellation on every send.
// It returns the scanner's error (nil on clean EOF); it does NOT close out.
func ScanLines(ctx context.Context, r io.Reader, source string, batchSize int, out chan<- data.RawBatch) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, DefaultInitLineBytes), MaxLineBytes)

	batch := make([][]byte, 0, batchSize)
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		select {
		case out <- data.RawBatch{Source: source, Records: batch}:
			batch = make([][]byte, 0, batchSize)
			return true
		case <-ctx.Done():
			return false
		}
	}

	for sc.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := append([]byte(nil), sc.Bytes()...)
		batch = append(batch, line)
		if len(batch) >= batchSize {
			if !flush() {
				return ctx.Err()
			}
		}
	}
	if !flush() {
		return ctx.Err()
	}
	return sc.Err()
}
