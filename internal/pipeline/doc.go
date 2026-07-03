// Package pipeline wires components from config into runnable pipelines and
// drives their concurrent lifecycle (docs/DESIGN.md §6).
//
// Build turns a *config.Config + a *component.Registry into a *Set: one
// builtPipeline per configured input. Run executes every pipeline
// concurrently — one worker per input — and returns once all have drained.
//
// Within a pipeline, each stage runs on its own goroutine connected by bounded
// channels, so channel capacity is the backpressure: a slow output stalls its
// branch, then fan-out, the buffer, the parser, and finally the input.
//
//	input → parser → [pre-processors] → buffer(window) → fan-out ⇒ each branch:
//	                                                        [procs] → encode → output
//
// The buffer accumulates parsed batches and flushes one window on the first of
// its bounds (age / rows / bytes). Fan-out hands each branch an independent
// reference (Retain) so branches release their columns independently.
//
// Pipelines are isolated: a fatal error (failure policy "block") stops only the
// offending pipeline; the rest keep running. Policy "drop" logs and skips the
// offending batch. Cancelling the run context stops every pipeline and
// components are shut down in reverse start order.
//
// This package depends only on the component interfaces + config + buffer +
// data — never on a concrete component package (dependencies point inward,
// docs/DESIGN.md §14).
package pipeline
