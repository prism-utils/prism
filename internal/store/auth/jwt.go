package auth

import (
	"context"
	"crypto"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	jose "github.com/go-jose/go-jose/v4"
)

// JWTConfig holds OIDC/JWKS settings for JWT verification.
type JWTConfig struct {
	Issuer   string
	JWKSURL  string
	JWKSFile string
	Audience []string
}

// Validate reports whether JWT verification can start with this configuration.
func (c *JWTConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("auth: jwt config is nil")
	}
	if strings.TrimSpace(c.Issuer) == "" {
		return fmt.Errorf("auth: OIDC_ISSUER is required")
	}
	if len(c.Audience) == 0 {
		return fmt.Errorf("auth: OIDC_AUDIENCE requires at least one value")
	}
	for _, aud := range c.Audience {
		if strings.TrimSpace(aud) == "" {
			return fmt.Errorf("auth: OIDC_AUDIENCE contains an empty entry")
		}
	}
	return nil
}

// JWTVerifier validates JWT bearer tokens against a JWKS-backed key set.
type JWTVerifier struct {
	verifier  *oidc.IDTokenVerifier
	audiences []string
}

// NewJWTVerifier builds a verifier from issuer discovery or static JWKS settings.
func NewJWTVerifier(ctx context.Context, cfg JWTConfig) (*JWTVerifier, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	oidcCfg := &oidc.Config{
		SkipClientIDCheck: true,
		Now:               time.Now,
	}
	var keySet oidc.KeySet
	switch {
	case cfg.JWKSFile != "":
		raw, err := os.ReadFile(cfg.JWKSFile) //nolint:gosec // G703: path comes from operator config
		if err != nil {
			return nil, fmt.Errorf("auth: read jwks file: %w", err)
		}
		keySet, err = staticKeySetFromJWKS(raw)
		if err != nil {
			return nil, err
		}
	case cfg.JWKSURL != "":
		keySet = oidc.NewRemoteKeySet(ctx, cfg.JWKSURL)
	default:
		provider, err := oidc.NewProvider(ctx, cfg.Issuer)
		if err != nil {
			return nil, fmt.Errorf("auth: oidc provider: %w", err)
		}
		return &JWTVerifier{
			verifier:  provider.Verifier(oidcCfg),
			audiences: append([]string(nil), cfg.Audience...),
		}, nil
	}
	return &JWTVerifier{
		verifier:  oidc.NewVerifier(cfg.Issuer, keySet, oidcCfg),
		audiences: append([]string(nil), cfg.Audience...),
	}, nil
}

// Verify validates the raw JWT and returns the subject claim.
func (v *JWTVerifier) Verify(ctx context.Context, rawToken string) (string, error) {
	if v == nil || v.verifier == nil {
		return "", fmt.Errorf("auth: verifier is not configured")
	}
	rawToken = strings.TrimSpace(rawToken)
	if rawToken == "" {
		return "", ErrMissingToken
	}
	idToken, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return "", mapVerifyError(err)
	}
	var claims struct {
		Audience audienceClaim `json:"aud"`
	}
	if err := idToken.Claims(&claims); err != nil {
		return "", fmt.Errorf("%w: %w", ErrMalformedToken, err)
	}
	if !audienceMatches(claims.Audience.tokenAudiences(), v.audiences) {
		return "", ErrWrongAudience
	}
	subject := strings.TrimSpace(idToken.Subject)
	if subject == "" {
		return "", ErrMissingSubject
	}
	return subject, nil
}

func mapVerifyError(err error) error {
	if err == nil {
		return nil
	}
	var expired *oidc.TokenExpiredError
	if errors.As(err, &expired) {
		return ErrExpired
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "signature"):
		return fmt.Errorf("%w: %w", ErrInvalidSignature, err)
	case strings.Contains(msg, "issuer"):
		return fmt.Errorf("%w: %w", ErrWrongIssuer, err)
	case strings.Contains(msg, "expired"):
		return fmt.Errorf("%w: %w", ErrExpired, err)
	default:
		return fmt.Errorf("%w: %w", ErrMalformedToken, err)
	}
}

type audienceClaim struct {
	items []string
}

func (a *audienceClaim) UnmarshalJSON(b []byte) error {
	var one string
	if err := json.Unmarshal(b, &one); err == nil {
		a.items = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return err
	}
	a.items = many
	return nil
}

func (a audienceClaim) tokenAudiences() []string {
	return a.items
}

func audienceMatches(tokenAudiences, expected []string) bool {
	if len(tokenAudiences) == 0 || len(expected) == 0 {
		return false
	}
	for _, got := range tokenAudiences {
		for _, want := range expected {
			if got == want {
				return true
			}
		}
	}
	return false
}

func staticKeySetFromJWKS(raw []byte) (oidc.KeySet, error) {
	var jwks jose.JSONWebKeySet
	if err := json.Unmarshal(raw, &jwks); err != nil {
		return nil, fmt.Errorf("auth: parse jwks: %w", err)
	}
	set, err := staticKeySetFromJWKSet(jwks)
	if err != nil {
		return nil, err
	}
	return set, nil
}

func staticKeySetFromJWKSet(jwks jose.JSONWebKeySet) (*oidc.StaticKeySet, error) {
	out := &oidc.StaticKeySet{}
	for i := range jwks.Keys {
		key := jwks.Keys[i]
		if key.Key == nil {
			continue
		}
		pk, ok := key.Key.(crypto.PublicKey)
		if !ok {
			continue
		}
		out.PublicKeys = append(out.PublicKeys, pk)
	}
	if len(out.PublicKeys) == 0 {
		return nil, fmt.Errorf("auth: jwks contains no usable public keys")
	}
	return out, nil
}
