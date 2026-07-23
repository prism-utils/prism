// Package authtest provides JWT and policy fixtures for store RBAC tests.
package authtest

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	jwt "github.com/go-jose/go-jose/v4/jwt"

	"github.com/elk-utilities/prism/internal/store/auth"
)

// JWTEnv hosts a JWKS endpoint and signs test tokens.
type JWTEnv struct {
	Issuer     string
	Audience   string
	Server     *httptest.Server
	JWKSPath   string
	privateKey *rsa.PrivateKey
}

// NewJWTEnv starts an httptest issuer with JWKS and discovery documents.
func NewJWTEnv(t *testing.T, audience string) *JWTEnv {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	env := &JWTEnv{
		Audience:   audience,
		privateKey: key,
		JWKSPath:   t.TempDir() + "/jwks.json",
	}
	pub := &jose.JSONWebKey{Key: key.Public(), Use: "sig", Algorithm: string(jose.RS256), KeyID: "test-kid"}
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{*pub}}
	raw, err := json.Marshal(jwks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFile(env.JWKSPath, raw); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		doc := map[string]string{
			"issuer":   env.IssuerURL(),
			"jwks_uri": env.IssuerURL() + "/jwks",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	})
	env.Server = httptest.NewServer(mux)
	env.Issuer = env.Server.URL
	t.Cleanup(env.Server.Close)
	return env
}

// IssuerURL returns the test issuer base URL.
func (e *JWTEnv) IssuerURL() string {
	return e.Server.URL
}

// Verifier returns a JWT verifier backed by the static JWKS file.
func (e *JWTEnv) Verifier(t *testing.T) *auth.JWTVerifier {
	t.Helper()
	v, err := auth.NewJWTVerifier(context.Background(), auth.JWTConfig{
		Issuer:   e.Issuer,
		JWKSFile: e.JWKSPath,
		Audience: []string{e.Audience},
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TokenOption mutates JWT claims before signing.
type TokenOption func(*jwtClaims)

// WithOmitSubject removes sub from the signed token.
func WithOmitSubject() TokenOption {
	return func(c *jwtClaims) { c.Subject = ""; c.omitSubject = true }
}

type jwtClaims struct {
	Subject     string
	Audience    jwt.Audience
	Issuer      string
	Expiry      *jwt.NumericDate
	omitSubject bool
}

// SignToken returns a bearer JWT string for tests.
func (e *JWTEnv) SignToken(opts ...TokenOption) (string, error) {
	claims := jwtClaims{
		Subject:  "alice@corp",
		Audience: jwt.Audience{e.Audience},
		Issuer:   e.Issuer,
		Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	for _, opt := range opts {
		opt(&claims)
	}
	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       &jose.JSONWebKey{Key: e.privateKey, KeyID: "test-kid", Algorithm: string(jose.RS256)},
	}, nil)
	if err != nil {
		return "", err
	}
	builder := jwt.Signed(signer)
	claimMap := map[string]any{
		"aud": claims.Audience,
		"iss": claims.Issuer,
		"exp": claims.Expiry,
		"iat": jwt.NewNumericDate(time.Now()),
	}
	if !claims.omitSubject {
		claimMap["sub"] = claims.Subject
	}
	return builder.Claims(claimMap).Serialize()
}

// WithSubject sets the sub claim.
func WithSubject(sub string) TokenOption {
	return func(c *jwtClaims) { c.Subject = sub }
}

// WithAudience replaces aud.
func WithAudience(aud ...string) TokenOption {
	return func(c *jwtClaims) { c.Audience = aud }
}

// WithIssuer replaces iss.
func WithIssuer(iss string) TokenOption {
	return func(c *jwtClaims) { c.Issuer = iss }
}

// WithExpiry sets exp.
func WithExpiry(when time.Time) TokenOption {
	return func(c *jwtClaims) { c.Expiry = jwt.NewNumericDate(when) }
}

// WithoutSubject clears sub.
func WithoutSubject() TokenOption {
	return func(c *jwtClaims) { c.Subject = "" }
}

func writeFile(path string, data []byte) error {
	return osWriteFile(path, data)
}
