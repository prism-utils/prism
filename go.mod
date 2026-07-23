module github.com/elk-utilities/prism

go 1.25.0

// Dependencies are added per-phase with `go get` at their latest release (never
// invented versions) and kept honest by `go mod tidy` in CI. The intended full
// set and the rationale for each library live in docs/DESIGN.md §13
// ("Dependency budget"); only the foundation subset is present so far.

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.47.0
	github.com/apache/arrow-go/v18 v18.6.0
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/knadh/koanf/parsers/yaml v1.1.0
	github.com/marcboeker/go-duckdb v1.8.5
	github.com/nxadm/tail v1.4.11
	github.com/stretchr/testify v1.11.1
	go.uber.org/goleak v1.3.0
	golang.org/x/sync v0.21.0
	google.golang.org/grpc v1.82.1
)

require (
	github.com/ClickHouse/ch-go v0.73.0 // indirect
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/apache/thrift v0.23.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/paulmach/orb v0.13.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rogpeppe/go-internal v1.15.0 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
