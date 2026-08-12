package flight

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/collect"
	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
	"github.com/prism-utils/prism/internal/tlsconf"
)

func TestConfig_Validate(t *testing.T) {
	if err := (&Config{}).Validate(); err == nil {
		t.Fatal("empty addr should be invalid")
	}
	if err := (&Config{Addr: "localhost:8815"}).Validate(); err != nil {
		t.Fatalf("valid addr rejected: %v", err)
	}
}

func TestDescriptorPath_EncodesProvenance(t *testing.T) {
	start := time.Date(2026, 7, 4, 1, 2, 3, 0, time.UTC)
	end := start.Add(time.Second)
	got := descriptorPath(&data.BlockMeta{
		Pipeline: "logs",
		Branch:   "template",
		Window:   data.TimeWindow{Start: start, End: end},
	})
	if len(got) != 4 {
		t.Fatalf("path len = %d, want 4", len(got))
	}
	if got[0] != "logs" || got[1] != "template" {
		t.Fatalf("provenance = %v", got[:2])
	}
	if got[2] != nano(start) || got[3] != nano(end) {
		t.Fatalf("window nanos = %v", got[2:])
	}
	if got[2] >= got[3] {
		t.Fatalf("start %s not before end %s", got[2], got[3])
	}
}

func TestDescriptorPath_NilAndZero(t *testing.T) {
	nilPath := descriptorPath(nil)
	if len(nilPath) != 4 || nilPath[0] != "unknown" || nilPath[2] != "0" {
		t.Fatalf("nil meta path = %v", nilPath)
	}
	zeroPath := descriptorPath(&data.BlockMeta{})
	if zeroPath[0] != "unknown" || zeroPath[2] != "0" || zeroPath[3] != "0" {
		t.Fatalf("zero meta path = %v", zeroPath)
	}
}

// startReceiver stands up an in-process collect Flight server and returns its
// bound address and ingest dir, cleaned up when the test ends.
func startReceiver(t *testing.T) (addr, dir string) {
	t.Helper()
	return startReceiverOpts(t)
}

func startReceiverOpts(t *testing.T, opts ...collect.Option) (addr, dir string) {
	t.Helper()
	dir = t.TempDir()
	srv, err := collect.NewServer(dir, nil, opts...)
	if err != nil {
		t.Fatalf("collect.NewServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	boundCh := make(chan string, 1)
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- srv.Serve(ctx, "127.0.0.1:0", func(b string) { boundCh <- b })
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-doneCh:
			if err != nil {
				t.Errorf("graceful shutdown returned error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("receiver did not stop after cancel")
		}
	})
	select {
	case addr = <-boundCh:
	case <-time.After(3 * time.Second):
		t.Fatal("receiver did not bind")
	}
	return addr, dir
}

// ipcBlock builds an Arrow IPC EncodedBlock from a small two-row batch, so the
// flight output has something to DoPut. mem is checked for leaks by the caller.
func ipcBlock(t *testing.T, mem memory.Allocator, meta *data.BlockMeta) data.EncodedBlock {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "route", Type: arrow.BinaryTypes.String},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	sb := array.NewStringBuilder(mem)
	defer sb.Release()
	ib := array.NewInt64Builder(mem)
	defer ib.Release()
	sb.Append("a")
	sb.Append("b")
	ib.Append(1)
	ib.Append(2)
	sc := sb.NewArray()
	defer sc.Release()
	ic := ib.NewArray()
	defer ic.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{sc, ic}, 2)
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		t.Fatalf("ipc write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ipc close: %v", err)
	}
	return data.EncodedBlock{Format: "arrow", Bytes: buf.Bytes(), Rows: 2, Meta: meta}
}

// The output DoPuts an IPC block to a live receiver, which persists it as a
// range-named Parquet carrying the descriptor provenance — the full transport.
func TestOutput_Consume_RoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	addr, dir := startReceiver(t)
	out, err := factory{}.Create(&Config{Addr: addr}, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := out.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = out.Shutdown(context.Background()) }()

	start := time.Now().UTC()
	meta := &data.BlockMeta{Pipeline: "metrics", Branch: "wire", Window: data.TimeWindow{Start: start, End: start.Add(time.Second)}}
	if err := out.Consume(context.Background(), ipcBlock(t, mem, meta)); err != nil {
		t.Fatalf("Consume: %v", err)
	}

	name := waitForParquet(t, dir)
	if !strings.HasPrefix(name, "metrics-wire-") {
		t.Fatalf("received file %q lacks descriptor provenance", name)
	}
}

// With a matching bearer token, the authenticated flight path round-trips to a
// token-guarded receiver.
func TestOutput_Consume_BearerAuth(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	addr, dir := startReceiverOpts(t, collect.WithToken("s3cr3t"))
	out, err := factory{}.Create(&Config{Addr: addr, Token: "s3cr3t"}, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := out.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = out.Shutdown(context.Background()) }()

	start := time.Now().UTC()
	meta := &data.BlockMeta{Pipeline: "metrics", Branch: "raw", Window: data.TimeWindow{Start: start, End: start.Add(time.Second)}}
	if err := out.Consume(context.Background(), ipcBlock(t, mem, meta)); err != nil {
		t.Fatalf("Consume with matching token: %v", err)
	}
	if name := waitForParquet(t, dir); !strings.HasPrefix(name, "metrics-raw-") {
		t.Fatalf("received file %q", name)
	}
}

// A missing/wrong token is rejected by the guarded receiver.
func TestOutput_Consume_BearerAuth_Rejected(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	addr, dir := startReceiverOpts(t, collect.WithToken("right"))
	out, _ := factory{}.Create(&Config{Addr: addr, Token: "wrong"}, component.Settings{})
	if err := out.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = out.Shutdown(context.Background()) }()

	start := time.Now().UTC()
	meta := &data.BlockMeta{Pipeline: "metrics", Branch: "raw", Window: data.TimeWindow{Start: start, End: start.Add(time.Second)}}
	if err := out.Consume(context.Background(), ipcBlock(t, mem, meta)); err == nil {
		t.Fatal("Consume with wrong token should be rejected")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("rejected stream still persisted %d files", len(entries))
	}
}

func TestConfig_ValidateTLS(t *testing.T) {
	if err := (&Config{Addr: "x:1", TLS: &tlsconf.Config{Cert: "c.pem"}}).Validate(); err == nil {
		t.Fatal("tls cert without key should be invalid")
	}
}

// An empty block is a no-op: no DoPut, no file, no error.
func TestOutput_Consume_EmptyBlock(t *testing.T) {
	addr, dir := startReceiver(t)
	out, _ := factory{}.Create(&Config{Addr: addr}, component.Settings{})
	if err := out.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = out.Shutdown(context.Background()) }()

	if err := out.Consume(context.Background(), data.EncodedBlock{Format: "arrow"}); err != nil {
		t.Fatalf("Consume empty: %v", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("empty block produced %d files", len(entries))
	}
}

func waitForParquet(t *testing.T, dir string) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".parquet" {
				return e.Name()
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no parquet appeared on receiver within timeout")
	return ""
}
