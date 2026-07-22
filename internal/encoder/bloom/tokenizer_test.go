package bloom

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenizeWords(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{name: "empty", in: "", want: nil},
		{name: "single token", in: "hello", want: []string{"hello"}},
		{name: "split punctuation", in: "foo, bar.baz!", want: []string{"foo", "bar", "baz"}},
		{name: "split whitespace", in: "  a   b  ", want: []string{"a", "b"}},
		{name: "digits kept", in: "trace-id-123", want: []string{"trace", "id", "123"}},
		{name: "non-ascii separators", in: "café résumé", want: []string{"caf", "r", "sum"}},
		{name: "single char", in: "x", want: []string{"x"}},
		{name: "only separators", in: "!!!---", want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := TokenizeWords(tc.in)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTokenizeTrigrams(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		n    int
		want []string
	}{
		{name: "empty", in: "", n: 3, want: nil},
		{name: "n zero", in: "abc", n: 0, want: nil},
		{name: "n one", in: "Ab", n: 1, want: []string{"a", "b"}},
		{name: "lowercased trigrams", in: "AbC", n: 3, want: []string{"abc"}},
		{name: "exact length", in: "abc", n: 3, want: []string{"abc"}},
		{name: "longer string", in: "abcd", n: 3, want: []string{"abc", "bcd"}},
		{name: "n larger than string", in: "ab", n: 3, want: nil},
		{name: "single char n3", in: "x", n: 3, want: nil},
		{name: "non-ascii bytes", in: "über", n: 3, want: []string{"übe", "ber"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := TokenizeTrigrams(tc.in, tc.n)
			assert.Equal(t, tc.want, got)
		})
	}
}
