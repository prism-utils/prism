package ingest_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/elk-utilities/prism/internal/duckdbfile"
	"github.com/elk-utilities/prism/internal/store/ingest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestFlightDoPut_DuckDBRawBytes(t *testing.T) {
	cfg := testConfig("", ingest.AuthNone)
	addr, eng := startFlightReceiver(t, &cfg)
	body := writeMetricsDuckDBWindow(t, "")

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := flight.NewClientFromConn(conn, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.DoPut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{testTenant, "metrics-raw", "0", "1", duckdbfile.FormatMeta},
	}
	if err := stream.Send(&flight.FlightData{
		FlightDescriptor: desc,
		AppMetadata:      []byte(duckdbfile.FormatMeta),
		DataBody:         body,
	}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1 after duckdb flight ingest", c)
	}
}
