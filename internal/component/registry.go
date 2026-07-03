package component

import (
	"errors"
	"fmt"
	"sort"
)

// Registry errors. Inspect with errors.Is.
var (
	// ErrDuplicateType is returned when two factories claim the same type.
	ErrDuplicateType = errors.New("component: duplicate factory type")
	// ErrUnknownType is returned when config references a type with no factory.
	ErrUnknownType = errors.New("component: unknown factory type")
	// ErrNilFactory is returned when a nil factory is registered.
	ErrNilFactory = errors.New("component: nil factory")
	// ErrEmptyType is returned when a factory reports an empty Type().
	ErrEmptyType = errors.New("component: empty factory type")
)

// Registry holds component factories keyed by kind and type. It is the single
// source of truth for turning a config "type" string into a component. Build
// one via NewRegistry and populate it through the components.Default() assembler
// (production) or directly with fakes (tests) — never through mandatory init().
type Registry struct {
	inputs     map[string]Factory[Input]
	parsers    map[string]Factory[Parser]
	processors map[string]Factory[Processor]
	encoders   map[string]Factory[Encoder]
	outputs    map[string]Factory[Output]
}

// NewRegistry returns an empty, ready-to-use Registry.
func NewRegistry() *Registry {
	return &Registry{
		inputs:     map[string]Factory[Input]{},
		parsers:    map[string]Factory[Parser]{},
		processors: map[string]Factory[Processor]{},
		encoders:   map[string]Factory[Encoder]{},
		outputs:    map[string]Factory[Output]{},
	}
}

// RegisterInput registers an input factory. It errors on nil, empty type, or a
// duplicate type.
func (r *Registry) RegisterInput(f Factory[Input]) error { return register(r.inputs, f) }

// RegisterParser registers a parser factory.
func (r *Registry) RegisterParser(f Factory[Parser]) error { return register(r.parsers, f) }

// RegisterProcessor registers a processor factory.
func (r *Registry) RegisterProcessor(f Factory[Processor]) error { return register(r.processors, f) }

// RegisterEncoder registers an encoder factory.
func (r *Registry) RegisterEncoder(f Factory[Encoder]) error { return register(r.encoders, f) }

// RegisterOutput registers an output factory.
func (r *Registry) RegisterOutput(f Factory[Output]) error { return register(r.outputs, f) }

// Input looks up an input factory by type.
func (r *Registry) Input(t string) (Factory[Input], error) { return lookup("input", r.inputs, t) }

// Parser looks up a parser factory by type.
func (r *Registry) Parser(t string) (Factory[Parser], error) { return lookup("parser", r.parsers, t) }

// Processor looks up a processor factory by type.
func (r *Registry) Processor(t string) (Factory[Processor], error) {
	return lookup("processor", r.processors, t)
}

// Encoder looks up an encoder factory by type.
func (r *Registry) Encoder(t string) (Factory[Encoder], error) {
	return lookup("encoder", r.encoders, t)
}

// Output looks up an output factory by type.
func (r *Registry) Output(t string) (Factory[Output], error) { return lookup("output", r.outputs, t) }

// register is the shared registration logic for every kind. Methods cannot be
// generic in Go, so the kind methods delegate to this free function.
func register[T any](m map[string]Factory[T], f Factory[T]) error {
	if f == nil {
		return ErrNilFactory
	}
	t := f.Type()
	if t == "" {
		return ErrEmptyType
	}
	if _, dup := m[t]; dup {
		return fmt.Errorf("%w: %q", ErrDuplicateType, t)
	}
	m[t] = f
	return nil
}

// lookup resolves a factory by type, returning ErrUnknownType with the list of
// known types when the type is absent — never a silent miss (docs/DESIGN.md §4).
func lookup[T any](kind string, m map[string]Factory[T], t string) (Factory[T], error) {
	f, ok := m[t]
	if !ok {
		return nil, fmt.Errorf("%w: kind=%s type=%q known=%v", ErrUnknownType, kind, t, sortedKeys(m))
	}
	return f, nil
}

func sortedKeys[T any](m map[string]Factory[T]) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
