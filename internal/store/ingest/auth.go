package ingest

import (
	"crypto/subtle"
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"
)

// ParseAuthMode parses a configured auth mode string.
func ParseAuthMode(s string) (AuthMode, error) {
	switch AuthMode(s) {
	case AuthNone, AuthBearer, AuthMTLS, AuthTrustedHeader:
		return AuthMode(s), nil
	default:
		return "", fmt.Errorf("ingest: unknown auth mode %q", s)
	}
}

// Authenticator resolves request identity for the configured auth mode.
type Authenticator struct {
	mode  AuthMode
	token string
}

// NewAuthenticator builds an Authenticator from ingest config.
func NewAuthenticator(cfg *Config) *Authenticator {
	return &Authenticator{mode: cfg.AuthMode, token: cfg.IngestToken}
}

// Authenticate reports whether the request passed auth and, for modes that
// carry a tenant identity, the authenticated tenant namespace.
func (a *Authenticator) Authenticate(r *http.Request) (ok bool, tenant string) {
	switch a.mode {
	case AuthNone:
		return true, ""
	case AuthBearer:
		if a.token == "" {
			return false, ""
		}
		return BearerEquals(r.Header.Get("Authorization"), a.token), ""
	case AuthTrustedHeader:
		h := strings.TrimSpace(r.Header.Get("X-Tenant"))
		if h == "" {
			return false, ""
		}
		return true, h
	case AuthMTLS:
		return mtlsIdentity(r)
	default:
		return false, ""
	}
}

func mtlsIdentity(r *http.Request) (bool, string) {
	if r.TLS == nil {
		return false, ""
	}
	var leaf *x509.Certificate
	if len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
		leaf = r.TLS.VerifiedChains[0][0]
	} else if len(r.TLS.PeerCertificates) > 0 {
		leaf = r.TLS.PeerCertificates[0]
	}
	if leaf == nil {
		return false, ""
	}
	cn := strings.TrimSpace(leaf.Subject.CommonName)
	if cn == "" {
		return false, ""
	}
	return true, cn
}

// BearerEquals compares an Authorization header to a static bearer token.
func BearerEquals(header, token string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if len(got) != len(token) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}
