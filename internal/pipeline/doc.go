// Package pipeline wires components from config into a runnable chain and
// drives their lifecycle (docs/DESIGN.md §6).
//
// Build turns a *config.Pipeline + a *component.Registry into a wired
// Pipeline: input → parser → [processor…] → encoder → output. Run starts the
// components, streams records through them until EOF or cancellation, and shuts
// them down in reverse order.
//
// Foundation scope: the input emits over its own bounded channel (backpressure
// between the source and the chain); processing is a single consumer loop and
// malformed data fails the run (fail-fast). Phase 2 of docs/PLAN.md expands
// this to per-stage goroutines with bounded channels between every stage and a
// configurable failure policy (drop | block | dead-letter).
//
// This package depends only on the component interfaces + config — never on a
// concrete component package (dependencies point inward, docs/DESIGN.md §14).
package pipeline
