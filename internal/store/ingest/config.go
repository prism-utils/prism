package ingest

// AuthMode selects how ingest requests are authenticated.
type AuthMode string

// Auth mode constants for ingest configuration.
const (
	AuthNone          AuthMode = "none"
	AuthBearer        AuthMode = "bearer"
	AuthMTLS          AuthMode = "mtls"
	AuthTrustedHeader AuthMode = "trusted-header"
)

// Config holds ingest validation and auth settings shared by HTTP and Flight.
type Config struct {
	AllowedArtifacts []string
	MaxBodyBytes     int64
	IngestToken      string
	AuthMode         AuthMode
	RoutePrefix      string
}
