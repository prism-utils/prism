package component_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// This file is the reference example of the repo's testing style
// (see docs/TESTING.md): table-driven where it helps, hand-written fakes over a
// mocking framework, sentinel-error assertions with errors.Is, and behavior —
// not implementation — under test.

// --- fakes -----------------------------------------------------------------

type fakeConfig struct{ valid bool }

func (c fakeConfig) Validate() error {
	if !c.valid {
		return errors.New("fakeConfig: invalid")
	}
	return nil
}

type fakeOutput struct{}

func (fakeOutput) Start(context.Context, component.Host) error      { return nil }
func (fakeOutput) Shutdown(context.Context) error                   { return nil }
func (fakeOutput) Consume(context.Context, data.EncodedBlock) error { return nil }

type fakeOutputFactory struct{ typ string }

func (f fakeOutputFactory) Type() string                  { return f.typ }
func (fakeOutputFactory) DefaultConfig() component.Config { return fakeConfig{valid: true} }
func (fakeOutputFactory) Create(component.Config, component.Settings) (component.Output, error) {
	return fakeOutput{}, nil
}

// --- tests -----------------------------------------------------------------

func TestRegistry_RegisterAndLookup(t *testing.T) {
	r := component.NewRegistry()

	if err := r.RegisterOutput(fakeOutputFactory{typ: "stdout"}); err != nil {
		t.Fatalf("RegisterOutput: unexpected error: %v", err)
	}

	got, err := r.Output("stdout")
	if err != nil {
		t.Fatalf("Output lookup: unexpected error: %v", err)
	}
	if got.Type() != "stdout" {
		t.Fatalf("Output lookup: got type %q, want %q", got.Type(), "stdout")
	}
}

func TestRegistry_RegisterErrors(t *testing.T) {
	tests := []struct {
		name    string
		factory component.Factory[component.Output]
		wantErr error
	}{
		{name: "nil factory", factory: nil, wantErr: component.ErrNilFactory},
		{name: "empty type", factory: fakeOutputFactory{typ: ""}, wantErr: component.ErrEmptyType},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := component.NewRegistry()
			err := r.RegisterOutput(tc.factory)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got error %v, want errors.Is(_, %v)", err, tc.wantErr)
			}
		})
	}
}

func TestRegistry_DuplicateType(t *testing.T) {
	r := component.NewRegistry()
	if err := r.RegisterOutput(fakeOutputFactory{typ: "http"}); err != nil {
		t.Fatalf("first register: %v", err)
	}
	err := r.RegisterOutput(fakeOutputFactory{typ: "http"})
	if !errors.Is(err, component.ErrDuplicateType) {
		t.Fatalf("got %v, want errors.Is(_, ErrDuplicateType)", err)
	}
}

func TestRegistry_UnknownTypeListsKnown(t *testing.T) {
	r := component.NewRegistry()
	_ = r.RegisterOutput(fakeOutputFactory{typ: "file"})
	_ = r.RegisterOutput(fakeOutputFactory{typ: "http"})

	_, err := r.Output("s3")
	if !errors.Is(err, component.ErrUnknownType) {
		t.Fatalf("got %v, want errors.Is(_, ErrUnknownType)", err)
	}
	// The error must help the operator by listing the known types.
	if msg := err.Error(); !strings.Contains(msg, "file") || !strings.Contains(msg, "http") {
		t.Fatalf("error %q should list known types file, http", msg)
	}
}

func TestRegistry_KindsAreIsolated(t *testing.T) {
	// Registering an output type must not make it resolvable as another kind.
	r := component.NewRegistry()
	_ = r.RegisterOutput(fakeOutputFactory{typ: "file"})

	if _, err := r.Input("file"); !errors.Is(err, component.ErrUnknownType) {
		t.Fatalf("output type leaked into input kind: err=%v", err)
	}
}
