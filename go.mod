module github.com/prism-utils/prism

go 1.25.8

toolchain go1.25.13

// Dependencies are added per-phase with `go get` at their latest release (never
// invented versions) and kept honest by `go mod tidy` in CI. The intended full
// set and the rationale for each library live in docs/DESIGN.md §13
// ("Dependency budget"); only the foundation subset is present so far.

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.48.0
	github.com/apache/arrow-go/v18 v18.7.0
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/coreos/go-oidc/v3 v3.20.0
	github.com/go-jose/go-jose/v4 v4.1.4
	github.com/knadh/koanf/parsers/yaml v1.1.1
	github.com/marcboeker/go-duckdb/v2 v2.4.3
	github.com/nxadm/tail v1.4.11
	github.com/prometheus/client_golang v1.24.1
	github.com/prometheus/common v0.70.1
	github.com/prometheus/prometheus v0.314.0
	github.com/shirou/gopsutil/v3 v3.24.5
	github.com/stretchr/testify v1.12.1
	go.uber.org/goleak v1.3.0
	golang.org/x/sync v0.22.0
	gonum.org/v1/plot v0.17.0
	google.golang.org/grpc v1.83.2
	gopkg.in/yaml.v3 v3.0.1
)

require (
	codeberg.org/go-fonts/liberation v0.5.0 // indirect
	codeberg.org/go-latex/latex v0.2.0 // indirect
	codeberg.org/go-pdf/fpdf v0.11.1 // indirect
	git.sr.ht/~sbinet/gg v0.7.0 // indirect
	github.com/ClickHouse/ch-go v0.74.0 // indirect
	github.com/ajstarks/svgo v0.0.0-20211024235047-1546f124cd8b // indirect
	github.com/andybalholm/brotli v1.2.2 // indirect
	github.com/apache/thrift v0.24.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/duckdb/duckdb-go-bindings v0.1.21 // indirect
	github.com/duckdb/duckdb-go-bindings/darwin-amd64 v0.1.21 // indirect
	github.com/duckdb/duckdb-go-bindings/darwin-arm64 v0.1.21 // indirect
	github.com/duckdb/duckdb-go-bindings/linux-amd64 v0.1.21 // indirect
	github.com/duckdb/duckdb-go-bindings/linux-arm64 v0.1.21 // indirect
	github.com/duckdb/duckdb-go-bindings/windows-amd64 v0.1.21 // indirect
	github.com/edsrzf/mmap-go v1.2.1-0.20241212181136-fad1cd13edbd // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20260330125221-c963978e514e // indirect
	github.com/marcboeker/go-duckdb/arrowmapping v0.0.21 // indirect
	github.com/marcboeker/go-duckdb/mapping v0.0.21 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/paulmach/orb v0.13.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/rogpeppe/go-internal v1.15.0 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/tklauser/go-sysconf v0.4.0 // indirect
	github.com/tklauser/numcpus v0.12.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/exp v0.0.0-20260709172345-9ea1abe57597 // indirect
	golang.org/x/image v0.30.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/term v0.45.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260720155508-bb71a54f79dc // indirect
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	k8s.io/apimachinery v0.35.3 // indirect
	k8s.io/client-go v0.35.3 // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
	k8s.io/kube-openapi v0.0.0-20260317180543-43fb72c5454a // indirect
	k8s.io/utils v0.0.0-20260210185600-b8788abfbbc2 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.3.3 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
)
