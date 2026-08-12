package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"strings"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	jwt "github.com/go-jose/go-jose/v4/jwt"

	"github.com/prism-utils/prism/internal/store/auth"
	"github.com/prism-utils/prism/internal/store/authtest"
)

func TestJWTVerifierValidToken(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	tok, err := env.SignToken(authtest.WithSubject("alice@corp"))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := v.Verify(context.Background(), tok)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if sub != "alice@corp" {
		t.Fatalf("subject = %q", sub)
	}
}

func TestJWTVerifierExpired(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	tok, err := env.SignToken(authtest.WithExpiry(time.Now().Add(-time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(context.Background(), tok)
	if !errors.Is(err, auth.ErrExpired) {
		t.Fatalf("err = %v, want ErrExpired", err)
	}
}

func TestJWTVerifierBadSignature(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: otherKey}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := jwt.Signed(signer).Claims(map[string]any{
		"sub": "alice@corp",
		"aud": []string{"prism-store"},
		"iss": env.Issuer,
		"exp": jwt.NewNumericDate(time.Now().Add(time.Hour)),
		"iat": jwt.NewNumericDate(time.Now()),
	}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(context.Background(), tok)
	if !errors.Is(err, auth.ErrInvalidSignature) && !errors.Is(err, auth.ErrMalformedToken) {
		t.Fatalf("err = %v, want signature failure", err)
	}
}

func TestJWTVerifierWrongAudience(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	tok, err := env.SignToken(authtest.WithAudience("other-aud"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(context.Background(), tok)
	if !errors.Is(err, auth.ErrWrongAudience) {
		t.Fatalf("err = %v, want ErrWrongAudience", err)
	}
}

func TestJWTVerifierWrongIssuer(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	tok, err := env.SignToken(authtest.WithIssuer("https://evil.example"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(context.Background(), tok)
	if !errors.Is(err, auth.ErrWrongIssuer) && !errors.Is(err, auth.ErrMalformedToken) {
		t.Fatalf("err = %v, want issuer failure", err)
	}
}

func TestJWTVerifierMissingSubject(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	tok, err := env.SignToken(authtest.WithOmitSubject())
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(context.Background(), tok)
	if !errors.Is(err, auth.ErrMissingSubject) {
		t.Fatalf("err = %v, want ErrMissingSubject", err)
	}
}

func TestJWTVerifierMalformed(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	_, err := v.Verify(context.Background(), "not-a-jwt")
	if !errors.Is(err, auth.ErrMalformedToken) {
		t.Fatalf("err = %v, want ErrMalformedToken", err)
	}
}

func TestJWTVerifierMissingToken(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	_, err := v.Verify(context.Background(), "")
	if !errors.Is(err, auth.ErrMissingToken) {
		t.Fatalf("err = %v, want ErrMissingToken", err)
	}
}

func TestJWTConfigValidate(t *testing.T) {
	cfg := auth.JWTConfig{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validate error for empty config")
	}
}

func TestJWTVerifierRejectsAlgNone(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	tok, err := authtest.UnsafeAlgNoneToken(env.Issuer, "prism-store", "attacker")
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(context.Background(), tok)
	if err == nil {
		t.Fatal("expected alg=none token to be rejected")
	}
	if !errors.Is(err, auth.ErrMalformedToken) && !errors.Is(err, auth.ErrInvalidSignature) {
		t.Fatalf("err = %v, want malformed or invalid signature", err)
	}
}

func TestJWTVerifierRejectsHMACAlgorithmConfusion(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	tok, err := authtest.HMACConfusionToken(env)
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(context.Background(), tok)
	if err == nil {
		t.Fatal("expected HMAC confusion token to be rejected")
	}
	if !errors.Is(err, auth.ErrInvalidSignature) && !errors.Is(err, auth.ErrMalformedToken) {
		t.Fatalf("err = %v, want signature or malformed failure", err)
	}
}

func TestJWTVerifierRejectsTamperedPayload(t *testing.T) {
	env := authtest.NewJWTEnv(t, "prism-store")
	v := env.Verifier(t)
	tok, err := env.SignToken(authtest.WithSubject("alice@corp"))
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatal("expected JWT compact form")
	}
	tampered := parts[0] + ".eyJzdWIiOiJhZG1pbiJ9." + parts[2]
	_, err = v.Verify(context.Background(), tampered)
	if err == nil {
		t.Fatal("expected tampered payload to be rejected")
	}
	if !errors.Is(err, auth.ErrInvalidSignature) && !errors.Is(err, auth.ErrMalformedToken) {
		t.Fatalf("err = %v, want signature failure", err)
	}
}
