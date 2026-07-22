package bloom

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalUnmarshal_roundTrip(t *testing.T) {
	t.Parallel()
	items := []string{"hello", "world", "foo", "bar"}
	orig := Build(items, 0.01)
	blob, err := orig.Marshal()
	require.NoError(t, err)

	decoded, err := Unmarshal(blob)
	require.NoError(t, err)
	for _, item := range items {
		assert.True(t, decoded.Contains(item), "round-trip missing %q", item)
	}
	assert.Equal(t, orig.m, decoded.m)
	assert.Equal(t, orig.k, decoded.k)
	assert.Equal(t, orig.nItems, decoded.nItems)
}

func TestParamsJSON(t *testing.T) {
	t.Parallel()
	p := Params{
		Version:   1,
		Hash:      "xxhash64",
		Combine:   "h1+i*h2",
		Tokenizer: "word",
		FPTarget:  0.01,
	}
	b, err := json.Marshal(p)
	require.NoError(t, err)
	assert.JSONEq(t, `{"version":1,"hash":"xxhash64","combine":"h1+i*h2","tokenizer":"word","fp_target":0.01}`, string(b))
}

func TestUnmarshal_emptyFilter(t *testing.T) {
	t.Parallel()
	orig := Build(nil, 0.01)
	blob, err := orig.Marshal()
	require.NoError(t, err)
	decoded, err := Unmarshal(blob)
	require.NoError(t, err)
	assert.False(t, decoded.Contains("x"))
}
