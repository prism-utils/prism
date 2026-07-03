package stdin

import (
	"context"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/data"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestInput_EmitsBoundedBatchesInOrder(t *testing.T) {
	in := &Input{
		cfg:     Config{BatchSize: 2},
		reader:  strings.NewReader("a\nb\nc\n"),
		batches: make(chan data.RawBatch, 1),
	}
	if err := in.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var got []string
	batchSizes := []int{}
	for b := range in.Batches() {
		batchSizes = append(batchSizes, b.Len())
		for _, rec := range b.Records {
			got = append(got, string(rec))
		}
	}

	want := []string{"a", "b", "c"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("records: got %v, want %v", got, want)
	}
	// batch_size=2 over 3 records => batches of [2,1].
	if len(batchSizes) != 2 || batchSizes[0] != 2 || batchSizes[1] != 1 {
		t.Fatalf("batch sizes: got %v, want [2 1]", batchSizes)
	}
}

func TestInput_CancelStopsProducerCleanly(t *testing.T) {
	// A cancelled context must stop the producer and close the channel without
	// leaking the goroutine (asserted by goleak in TestMain).
	ctx, cancel := context.WithCancel(context.Background())
	in := &Input{
		cfg:     Config{BatchSize: 1},
		reader:  strings.NewReader("x\ny\nz\n"),
		batches: make(chan data.RawBatch, 1),
	}
	if err := in.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancel()
	// Drain until closed; must terminate.
	for range in.Batches() {
	}
}

func TestConfig_Validate(t *testing.T) {
	if err := (&Config{BatchSize: 0}).Validate(); err == nil {
		t.Fatal("expected error for batch_size 0")
	}
	if err := (&Config{BatchSize: 10}).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
