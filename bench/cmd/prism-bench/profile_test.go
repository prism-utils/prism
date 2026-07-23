package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveProfile_baseline(t *testing.T) {
	t.Parallel()
	p, err := resolveProfile(false, false)
	require.NoError(t, err)
	require.Equal(t, "", p)
}

func TestResolveProfile_api(t *testing.T) {
	t.Parallel()
	p, err := resolveProfile(true, false)
	require.NoError(t, err)
	require.Equal(t, "api", p)
}

func TestResolveProfile_apiArrow(t *testing.T) {
	t.Parallel()
	p, err := resolveProfile(true, true)
	require.NoError(t, err)
	require.Equal(t, "api-arrow", p)
}

func TestResolveProfile_arrowWithoutAPI_errors(t *testing.T) {
	t.Parallel()
	_, err := resolveProfile(false, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--arrow requires --api")
}
