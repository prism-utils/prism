package authgen_test

import (
	"context"
	"testing"
	"time"

	"github.com/elk-utilities/prism/bench/internal/authgen"
	"github.com/elk-utilities/prism/internal/store/auth"
	"github.com/stretchr/testify/require"
)

func TestMintedJWTVerifiesAgainstJWKS(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	tenant := "bench-tenant"

	env, err := authgen.New(dir, tenant)
	require.NoError(t, err)

	tok, err := env.Token()
	require.NoError(t, err)

	verifier, err := auth.NewJWTVerifier(ctx, auth.JWTConfig{
		Issuer:   env.Issuer(),
		JWKSFile: env.JWKSPath(),
		Audience: []string{env.Audience()},
	})
	require.NoError(t, err)

	sub, err := verifier.Verify(ctx, tok)
	require.NoError(t, err)
	require.Equal(t, authgen.AdminSubject, sub)
}

func TestShortLivedTokenRejected(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	env, err := authgen.New(dir, "bench-tenant")
	require.NoError(t, err)

	tok, err := env.ExpiredToken()
	require.NoError(t, err)

	verifier, err := auth.NewJWTVerifier(ctx, auth.JWTConfig{
		Issuer:   env.Issuer(),
		JWKSFile: env.JWKSPath(),
		Audience: []string{env.Audience()},
	})
	require.NoError(t, err)

	_, err = verifier.Verify(ctx, tok)
	require.Error(t, err)
	require.ErrorIs(t, err, auth.ErrExpired)
}

func TestPolicyFileWritten(t *testing.T) {
	dir := t.TempDir()
	tenant := "my-tenant"

	env, err := authgen.New(dir, tenant)
	require.NoError(t, err)

	raw, err := env.ReadPolicy()
	require.NoError(t, err)
	require.Contains(t, string(raw), authgen.AdminSubject)
	require.Contains(t, string(raw), tenant)
	require.Contains(t, string(raw), "admin")
}

func TestTokenForSubjectUsesCustomPolicy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	tenant := "bench-tenant"
	other := "other-tenant"

	env, err := authgen.New(dir, tenant)
	require.NoError(t, err)

	policy := authgen.PolicyYAML([]authgen.Binding{
		{Subject: "reader-a", Role: "reader", Tenants: []string{tenant}},
		{Subject: "writer-a", Role: "writer", Tenants: []string{tenant}},
		{Subject: "reader-b", Role: "reader", Tenants: []string{other}},
	})
	require.NoError(t, env.WritePolicy(policy))

	verifier, err := auth.NewJWTVerifier(ctx, auth.JWTConfig{
		Issuer:   env.Issuer(),
		JWKSFile: env.JWKSPath(),
		Audience: []string{env.Audience()},
	})
	require.NoError(t, err)

	readerTok, err := env.TokenFor("reader-a", time.Hour)
	require.NoError(t, err)
	sub, err := verifier.Verify(ctx, readerTok)
	require.NoError(t, err)
	require.Equal(t, "reader-a", sub)
}
