// Package components is the single assembler that registers prism's built-in
// components into a Registry. Production wires this; tests build their own
// Registry with just the fakes they need. This is the "no mandatory init()"
// rule from docs/DESIGN.md §4 — registration is explicit and injectable.
package components

import (
	"fmt"

	"github.com/elk-utilities/prism/internal/component"
	encoderjson "github.com/elk-utilities/prism/internal/encoder/json"
	encoderparquet "github.com/elk-utilities/prism/internal/encoder/parquet"
	encoderraw "github.com/elk-utilities/prism/internal/encoder/raw"
	inputfile "github.com/elk-utilities/prism/internal/input/file"
	inputprom "github.com/elk-utilities/prism/internal/input/prometheus"
	"github.com/elk-utilities/prism/internal/input/stdin"
	outputdir "github.com/elk-utilities/prism/internal/output/dir"
	outputfile "github.com/elk-utilities/prism/internal/output/file"
	"github.com/elk-utilities/prism/internal/output/stdout"
	parserjson "github.com/elk-utilities/prism/internal/parser/json"
	parserlogfmt "github.com/elk-utilities/prism/internal/parser/logfmt"
	parserprom "github.com/elk-utilities/prism/internal/parser/prometheus"
	parserraw "github.com/elk-utilities/prism/internal/parser/raw"
	parserregex "github.com/elk-utilities/prism/internal/parser/regex"
	procsummary "github.com/elk-utilities/prism/internal/processor/summary"
	proctemplate "github.com/elk-utilities/prism/internal/processor/template"
)

// Default returns a Registry populated with every built-in component. Adding a
// new component is one line here plus its package — no other core edits.
func Default() (*component.Registry, error) {
	reg := component.NewRegistry()

	for _, err := range []error{
		reg.RegisterInput(stdin.NewFactory()),
		reg.RegisterInput(inputfile.NewFactory()),
		reg.RegisterInput(inputprom.NewFactory()),
		reg.RegisterParser(parserraw.NewFactory()),
		reg.RegisterParser(parserprom.NewFactory()),
		reg.RegisterParser(parserlogfmt.NewFactory()),
		reg.RegisterParser(parserjson.NewFactory()),
		reg.RegisterParser(parserregex.NewFactory()),
		reg.RegisterProcessor(procsummary.NewFactory()),
		reg.RegisterProcessor(proctemplate.NewFactory()),
		reg.RegisterEncoder(encoderraw.NewFactory()),
		reg.RegisterEncoder(encoderjson.NewFactory()),
		reg.RegisterEncoder(encoderparquet.NewFactory()),
		reg.RegisterOutput(stdout.NewFactory()),
		reg.RegisterOutput(outputfile.NewFactory()),
		reg.RegisterOutput(outputdir.NewFactory()),
	} {
		if err != nil {
			return nil, fmt.Errorf("components: %w", err)
		}
	}

	return reg, nil
}
