package bloom

import (
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashBytes_matchesStringPath(t *testing.T) {
	t.Parallel()
	cases := []string{
		"hello world foo",
		"über café résumé",
		"trace-id-123",
	}
	for _, s := range cases {
		b := []byte(s)
		setStr := make(map[uint64]struct{})
		for _, tok := range TokenizeWords(s) {
			setStr[xxhash.Sum64String(tok)] = struct{}{}
		}
		setBytes := make(map[uint64]struct{})
		AddWordHashesBytes(setBytes, b)
		assert.Equal(t, setStr, setBytes, "word hashes for %q", s)

		setStrTri := make(map[uint64]struct{})
		for _, tri := range TokenizeTrigrams(s, 3) {
			setStrTri[xxhash.Sum64String(tri)] = struct{}{}
		}
		setBytesTri := make(map[uint64]struct{})
		var scratch []byte
		var offsets []int
		_, _ = AddTrigramHashesBytes(setBytesTri, scratch, b, offsets, 3)
		assert.Equal(t, setStrTri, setBytesTri, "trigram hashes for %q", s)
	}
}

func TestAddTrigramHashesBytes_nonASCII_containsRoundTrip(t *testing.T) {
	t.Parallel()
	const n = 3
	msg := "über café"
	set := make(map[uint64]struct{})
	var scratch []byte
	var offsets []int
	_, _ = AddTrigramHashesBytes(set, scratch, []byte(msg), offsets, n)
	require.NotEmpty(t, set)
	f := BuildFromHashes(set, 0.01)
	for tri := range allTrigramsFromString(msg, n) {
		assert.True(t, f.Contains(tri), "missing trigram %q", tri)
	}
}

func allTrigramsFromString(s string, n int) map[string]struct{} {
	out := make(map[string]struct{})
	for _, tri := range TokenizeTrigrams(s, n) {
		out[tri] = struct{}{}
	}
	return out
}
