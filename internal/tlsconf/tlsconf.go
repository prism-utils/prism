// Package tlsconf is the shared client-TLS configuration block used by any
// component that dials a TLS endpoint (scrape inputs, network outputs). It is a
// plain utility — not a pipeline component — so components can reuse one
// consistent `tls { ca, cert, key, server_name, insecure_skip_verify }` surface
// without importing one another.
package tlsconf

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Config is the client-side TLS block. File fields are paths read by Build (at
// component Start), so certificate/key material never lives in the config file.
type Config struct {
	// CA is a PEM bundle used to verify the server certificate. Empty uses the
	// host's system roots.
	CA string `json:"ca"`
	// Cert and Key are the client certificate/key for mTLS (both or neither).
	Cert string `json:"cert"`
	Key  string `json:"key"`
	// ServerName overrides the SNI / verification hostname.
	ServerName string `json:"server_name"`
	// InsecureSkipVerify disables server-certificate verification. It is for
	// self-signed development endpoints only and is never safe in production.
	InsecureSkipVerify bool `json:"insecure_skip_verify"`
}

// Validate reports logical faults (cert/key must be paired). It does not touch
// the filesystem; missing files surface from Build at Start. path names the
// offending config location for the operator. A nil receiver is valid (no TLS).
func (c *Config) Validate(path string) error {
	if c == nil {
		return nil
	}
	if (c.Cert == "") != (c.Key == "") {
		return fmt.Errorf("%s: cert and key must be set together", path)
	}
	return nil
}

// Build reads the configured CA/client-cert material into a *tls.Config. A nil
// receiver returns a nil config (meaning "use transport defaults").
func (c *Config) Build() (*tls.Config, error) {
	if c == nil {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.InsecureSkipVerify, //nolint:gosec // opt-in for self-signed dev endpoints; documented as unsafe
	}
	if c.CA != "" {
		pem, err := os.ReadFile(c.CA)
		if err != nil {
			return nil, fmt.Errorf("read ca %q: %w", c.CA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca %q: no certificates found", c.CA)
		}
		cfg.RootCAs = pool
	}
	if c.Cert != "" {
		pair, err := tls.LoadX509KeyPair(c.Cert, c.Key)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}
