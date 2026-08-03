package ingest_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/ingest"
)

func startFlightReceiver(t *testing.T, cfg *ingest.Config) (addr string, eng *engine.Engine) {
	t.Helper()
	eng = engine.New(engine.Config{DataDir: t.TempDir(), HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := ingest.NewFlightServer(cfg, eng, logger)
	if err != nil {
		t.Fatalf("NewFlightServer: %v", err)
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
				t.Errorf("flight shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("flight server did not stop")
		}
	})
	select {
	case addr = <-boundCh:
	case <-time.After(3 * time.Second):
		t.Fatal("flight server did not bind")
	}
	return addr, eng
}

func metricsIPCBlock(t *testing.T, mem memory.Allocator) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "__name__", Type: arrow.BinaryTypes.String},
		{Name: "labels", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "timestamp_ms", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	nb := array.NewStringBuilder(mem)
	lb := array.NewStringBuilder(mem)
	vb := array.NewFloat64Builder(mem)
	tb := array.NewInt64Builder(mem)
	defer nb.Release()
	defer lb.Release()
	defer vb.Release()
	defer tb.Release()
	nb.Append("up")
	lb.Append("{}")
	vb.Append(1)
	tb.Append(0)
	nc := nb.NewArray()
	lc := lb.NewArray()
	vc := vb.NewArray()
	tc := tb.NewArray()
	defer nc.Release()
	defer lc.Release()
	defer vc.Release()
	defer tc.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{nc, lc, vc, tc}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		t.Fatalf("ipc write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ipc close: %v", err)
	}
	return buf.Bytes()
}

func logsSummaryIPCBlock(t *testing.T, mem memory.Allocator) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "template", Type: arrow.BinaryTypes.String},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	tb := array.NewStringBuilder(mem)
	cb := array.NewInt64Builder(mem)
	defer tb.Release()
	defer cb.Release()
	tb.Append("user <*> logged in")
	cb.Append(3)
	tc := tb.NewArray()
	cc := cb.NewArray()
	defer tc.Release()
	defer cc.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{tc, cc}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		t.Fatalf("ipc write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ipc close: %v", err)
	}
	return buf.Bytes()
}

func doPutWindow(t *testing.T, addr, token, tenant, artifact string, ipcBytes []byte) error {
	t.Helper()
	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if token != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(bearerCreds{token: token}))
	}
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, dialOpts...)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	stream, err := client.DoPut(ctx)
	if err != nil {
		return err
	}
	rdr, err := ipc.NewReader(bytes.NewReader(ipcBytes))
	if err != nil {
		return err
	}
	defer rdr.Release()
	w := flight.NewRecordWriter(stream, ipc.WithSchema(rdr.Schema()))
	w.SetFlightDescriptor(&flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{tenant, artifact, "0", "1"},
	})
	for rdr.Next() {
		if err := w.Write(rdr.RecordBatch()); err != nil {
			_ = w.Close()
			return err
		}
	}
	if err := rdr.Err(); err != nil {
		_ = w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

type bearerCreds struct {
	token string
}

func (b bearerCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearerCreds) RequireTransportSecurity() bool { return false }

func TestFlightDoPutRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	cfg := testConfig("", ingest.AuthNone)
	addr, eng := startFlightReceiver(t, &cfg)
	ipcBytes := metricsIPCBlock(t, mem)
	if err := doPutWindow(t, addr, "", testTenant, "metrics-raw", ipcBytes); err != nil {
		t.Fatalf("DoPut: %v", err)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
}

// TestFlightDoPutLogsLandsWithoutHotRows proves the Flight path routes logs
// artifacts to the land-as-file path instead of the metrics hot catalog, so a
// shared ALLOWED_ARTIFACTS list cannot make Flight fail on logs.
func TestFlightDoPutLogsLandsWithoutHotRows(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	cfg := ingest.Config{
		AllowedArtifacts: []string{"metrics-raw", "logs-summary"},
		MaxBodyBytes:     1 << 20,
		AuthMode:         ingest.AuthNone,
	}
	addr, eng := startFlightReceiver(t, &cfg)
	ipcBytes := logsSummaryIPCBlock(t, mem)
	if err := doPutWindow(t, addr, "", testTenant, "logs-summary", ipcBytes); err != nil {
		t.Fatalf("DoPut logs-summary: %v", err)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 0 {
		t.Fatalf("hot rows = %d, want 0 (logs land as files, not metrics)", c)
	}
}

func TestFlightDoPutBearerRejected(t *testing.T) {
	cfg := testConfig("s3cret", ingest.AuthBearer)
	addr, eng := startFlightReceiver(t, &cfg)
	ipcBytes := metricsIPCBlock(t, memory.DefaultAllocator)
	if err := doPutWindow(t, addr, "wrong", testTenant, "metrics-raw", ipcBytes); err == nil {
		t.Fatal("DoPut with wrong token should fail")
	}
	if c, _ := eng.HotRowCount(testTenant); c != 0 {
		t.Fatalf("hot rows = %d, want 0", c)
	}
}

func TestFlightDoPutUnknownTenant(t *testing.T) {
	cfg := testConfig("", ingest.AuthNone)
	addr, eng := startFlightReceiver(t, &cfg)
	ipcBytes := metricsIPCBlock(t, memory.DefaultAllocator)
	if err := doPutWindow(t, addr, "", "../bad", "metrics-raw", ipcBytes); err == nil {
		t.Fatal("DoPut unknown tenant should fail")
	}
	if c, _ := eng.HotRowCount("../bad"); c != 0 {
		t.Fatalf("hot rows = %d, want 0", c)
	}
}
