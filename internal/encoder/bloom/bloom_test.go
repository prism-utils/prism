package bloom

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuild_emptySet(t *testing.T) {
	t.Parallel()
	f := Build(nil, 0.01)
	require.NotNil(t, f)
	assert.False(t, f.Contains("anything"))
}

func TestBuild_noFalseNegatives(t *testing.T) {
	t.Parallel()
	items := []string{"alpha", "beta", "gamma", "delta123", "trace-id-99"}
	f := Build(items, 0.01)
	for _, item := range items {
		assert.True(t, f.Contains(item), "missing %q", item)
	}
}

func TestBuild_falsePositiveRate(t *testing.T) {
	t.Parallel()
	const fpTarget = 0.01
	items := make([]string, 500)
	for i := range items {
		items[i] = fmt.Sprintf("item-%d-unique", i)
	}
	f := Build(items, fpTarget)

	const probes = 50_000
	falsePos := 0
	for i := range probes {
		candidate := fmt.Sprintf("probe-%d-absent", i)
		if f.Contains(candidate) {
			falsePos++
		}
	}
	measured := float64(falsePos) / float64(probes)
	// Allow generous tolerance: sampling variance + small-n effects.
	assert.InDelta(t, fpTarget, measured, 0.008, "measured FP=%v", measured)
}

func TestBuild_deterministic(t *testing.T) {
	t.Parallel()
	items := []string{"one", "two", "three"}
	a := Build(items, 0.05)
	b := Build(items, 0.05)
	aBlob, err := a.Marshal()
	require.NoError(t, err)
	bBlob, err := b.Marshal()
	require.NoError(t, err)
	assert.Equal(t, aBlob, bBlob)
}

func TestBuild_sizing(t *testing.T) {
	t.Parallel()
	n := 100
	fp := 0.01
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf("tok-%d", i)
	}
	f := Build(items, fp)
	ln2 := math.Ln2
	wantM := int(math.Ceil(-float64(n) * math.Log(fp) / (ln2 * ln2)))
	wantK := max(1, int(math.Round(float64(wantM)/float64(n)*ln2)))
	assert.Equal(t, wantM, f.m)
	assert.Equal(t, wantK, f.k)
	assert.Equal(t, n, f.nItems)
}
