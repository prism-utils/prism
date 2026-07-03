module github.com/elk-utilities/prism

go 1.25.0

// Dependencies are added per-phase with `go get` at their latest release (never
// invented versions) and kept honest by `go mod tidy` in CI. The intended full
// set and the rationale for each library live in docs/DESIGN.md §13
// ("Dependency budget"); only the foundation subset is present so far.

require (
	go.uber.org/goleak v1.3.0
	golang.org/x/sync v0.21.0
)
