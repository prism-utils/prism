package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveProfile_baseline(t *testing.T) {
	t.Parallel()
	p, err := resolveProfile(false, false, false)
	require.NoError(t, err)
	require.Equal(t, "", p)
}

func TestResolveProfile_api(t *testing.T) {
	t.Parallel()
	p, err := resolveProfile(true, false, false)
	require.NoError(t, err)
	require.Equal(t, "api", p)
}

func TestResolveProfile_apiArrow(t *testing.T) {
	t.Parallel()
	p, err := resolveProfile(true, true, false)
	require.NoError(t, err)
	require.Equal(t, "api-arrow", p)
}

func TestResolveProfile_arrowWithoutAPI_errors(t *testing.T) {
	t.Parallel()
	_, err := resolveProfile(false, true, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--arrow requires --api")
}

func TestResolveProfile_apiHot(t *testing.T) {
	t.Parallel()
	p, err := resolveProfile(true, false, true)
	require.NoError(t, err)
	require.Equal(t, "api-hot", p)
}

func TestResolveProfile_apiArrowHot(t *testing.T) {
	t.Parallel()
	p, err := resolveProfile(true, true, true)
	require.NoError(t, err)
	require.Equal(t, "api-arrow-hot", p)
}

func TestResolveProfile_hotOnlyWithoutAPI_errors(t *testing.T) {
	t.Parallel()
	_, err := resolveProfile(false, false, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--hot-only requires --api")
}
