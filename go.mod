module github.com/elk-utilities/prism

go 1.25.0

// Dependencies are added per-phase with `go get` at their latest release (never
// invented versions) and kept honest by `go mod tidy` in CI. The intended full
// set and the rationale for each library live in docs/DESIGN.md §13
// ("Dependency budget"); only the foundation subset is present so far.

require (
	github.com/apache/arrow-go/v18 v18.6.0
	github.com/knadh/koanf/parsers/yaml v1.1.0
	go.uber.org/goleak v1.3.0
	golang.org/x/sync v0.21.0
)

require (
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.yaml.in/yaml/v3 v3.0.3 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/sys v0.43.0 // indirect
)
