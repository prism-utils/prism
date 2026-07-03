// Package components is the single assembler that registers prism's built-in
// components into a Registry. Production wires this; tests build their own
// Registry with just the fakes they need. This is the "no mandatory init()"
// rule from docs/DESIGN.md §4 — registration is explicit and injectable.
package components

import (
	"fmt"

	"github.com/elk-utilities/prism/internal/component"
	encoderraw "github.com/elk-utilities/prism/internal/encoder/raw"
	inputfile "github.com/elk-utilities/prism/internal/input/file"
	"github.com/elk-utilities/prism/internal/input/stdin"
	outputfile "github.com/elk-utilities/prism/internal/output/file"
	"github.com/elk-utilities/prism/internal/output/stdout"
	parserraw "github.com/elk-utilities/prism/internal/parser/raw"
)

// Default returns a Registry populated with every built-in component. Adding a
// new component is one line here plus its package — no other core edits.
func Default() (*component.Registry, error) {
	reg := component.NewRegistry()

	if err := reg.RegisterInput(stdin.NewFactory()); err != nil {
		return nil, fmt.Errorf("components: %w", err)
	}
	if err := reg.RegisterInput(inputfile.NewFactory()); err != nil {
		return nil, fmt.Errorf("components: %w", err)
	}
	if err := reg.RegisterParser(parserraw.NewFactory()); err != nil {
		return nil, fmt.Errorf("components: %w", err)
	}
	if err := reg.RegisterEncoder(encoderraw.NewFactory()); err != nil {
		return nil, fmt.Errorf("components: %w", err)
	}
	if err := reg.RegisterOutput(stdout.NewFactory()); err != nil {
		return nil, fmt.Errorf("components: %w", err)
	}
	if err := reg.RegisterOutput(outputfile.NewFactory()); err != nil {
		return nil, fmt.Errorf("components: %w", err)
	}

	return reg, nil
}
