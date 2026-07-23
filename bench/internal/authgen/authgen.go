// Package authgen mints RSA JWKS files, RBAC policy YAML, and RS256 JWTs for the
// benchmark API profile without test-only fixtures.
package authgen

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	jwt "github.com/go-jose/go-jose/v4/jwt"
)

const (
	// DefaultIssuer is the OIDC issuer written into bench JWTs and store env.
	DefaultIssuer = "https://bench.prism.local/issuer"
	// DefaultAudience matches prism-store RBAC tests and production config.
	DefaultAudience = "prism-store"
	// AdminSubject is the bench principal granted admin on the bench tenant.
	AdminSubject = "bench-admin"
)

// Binding is one RBAC policy row.
type Binding struct {
	Subject string
	Role    string
	Tenants []string
}

// Env holds runtime key material and on-disk JWKS/policy paths.
type Env struct {
	issuer     string
	audience   string
	tenant     string
	workDir    string
	keyID      string
	privateKey *rsa.PrivateKey
	jwksPath   string
	policyPath string
}

// New generates RSA key material, writes JWKS and a default admin policy for tenant.
func New(workDir, tenant string) (*Env, error) {
	if strings.TrimSpace(workDir) == "" {
		return nil, fmt.Errorf("authgen: empty work dir")
	}
	if strings.TrimSpace(tenant) == "" {
		return nil, fmt.Errorf("authgen: empty tenant")
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("authgen: rsa keygen: %w", err)
	}
	env := &Env{
		issuer:     DefaultIssuer,
		audience:   DefaultAudience,
		tenant:     tenant,
		workDir:    workDir,
		keyID:      "bench-kid",
		privateKey: key,
		jwksPath:   filepath.Join(workDir, "jwks.json"),
		policyPath: filepath.Join(workDir, "policy.yaml"),
	}
	if err := env.writeJWKS(); err != nil {
		return nil, err
	}
	if err := env.WritePolicy(PolicyYAML([]Binding{
		{Subject: AdminSubject, Role: "admin", Tenants: []string{tenant}},
	})); err != nil {
		return nil, err
	}
	return env, nil
}

// Issuer returns the configured OIDC issuer string.
func (e *Env) Issuer() string { return e.issuer }

// Audience returns the accepted JWT audience.
func (e *Env) Audience() string { return e.audience }

// JWKSPath returns the on-disk JWKS JSON path.
func (e *Env) JWKSPath() string { return e.jwksPath }

// PolicyPath returns the on-disk RBAC policy YAML path.
func (e *Env) PolicyPath() string { return e.policyPath }

// ReadPolicy returns the current policy file bytes.
func (e *Env) ReadPolicy() ([]byte, error) {
	raw, err := os.ReadFile(e.policyPath) //nolint:gosec // G703: path under bench work dir
	if err != nil {
		return nil, fmt.Errorf("authgen: read policy: %w", err)
	}
	return raw, nil
}

// WritePolicy persists bindings as deny-by-default RBAC YAML.
func (e *Env) WritePolicy(body string) error {
	if err := os.WriteFile(e.policyPath, []byte(body), 0o600); err != nil {
		return fmt.Errorf("authgen: write policy: %w", err)
	}
	return nil
}

// PolicyYAML formats bindings into store RBAC policy YAML.
func PolicyYAML(bindings []Binding) string {
	var b strings.Builder
	b.WriteString("bindings:\n")
	for _, bind := range bindings {
		fmt.Fprintf(&b, "  - subject: %q\n", bind.Subject)
		fmt.Fprintf(&b, "    role: %s\n", bind.Role)
		b.WriteString("    tenants: [")
		for i, t := range bind.Tenants {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%q", t)
		}
		b.WriteString("]\n")
	}
	return b.String()
}

// Token returns a signed JWT for the default admin subject.
func (e *Env) Token() (string, error) {
	return e.TokenFor(AdminSubject, time.Hour)
}

// ExpiredToken returns a JWT that is already past exp for negative verification tests.
func (e *Env) ExpiredToken() (string, error) {
	return e.sign(AdminSubject, time.Now().Add(-time.Hour))
}

// TokenFor mints a JWT for subject with the given lifetime from now.
func (e *Env) TokenFor(subject string, ttl time.Duration) (string, error) {
	return e.sign(subject, time.Now().Add(ttl))
}

func (e *Env) writeJWKS() error {
	pub := jose.JSONWebKey{
		Key:       e.privateKey.Public(),
		Use:       "sig",
		Algorithm: string(jose.RS256),
		KeyID:     e.keyID,
	}
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{pub}}
	raw, err := json.Marshal(jwks)
	if err != nil {
		return fmt.Errorf("authgen: marshal jwks: %w", err)
	}
	if err := os.WriteFile(e.jwksPath, raw, 0o600); err != nil {
		return fmt.Errorf("authgen: write jwks: %w", err)
	}
	return nil
}

func (e *Env) sign(subject string, exp time.Time) (string, error) {
	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key: &jose.JSONWebKey{
			Key:       e.privateKey,
			KeyID:     e.keyID,
			Algorithm: string(jose.RS256),
		},
	}, nil)
	if err != nil {
		return "", fmt.Errorf("authgen: signer: %w", err)
	}
	now := time.Now()
	claims := map[string]any{
		"iss": e.issuer,
		"aud": jwt.Audience{e.audience},
		"sub": subject,
		"exp": jwt.NewNumericDate(exp),
		"iat": jwt.NewNumericDate(now),
	}
	tok, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		return "", fmt.Errorf("authgen: sign jwt: %w", err)
	}
	return tok, nil
}
