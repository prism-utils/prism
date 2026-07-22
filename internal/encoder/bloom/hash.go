package bloom

import (
	"strings"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
)

// AddWordHashes inserts xxhash64 keys for word tokens of s into set.
func AddWordHashes(set map[uint64]struct{}, s string) {
	if s == "" {
		return
	}
	start := -1
	for i, r := range s {
		if isWordChar(r) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			set[xxhash.Sum64String(s[start:i])] = struct{}{}
			start = -1
		}
	}
	if start >= 0 {
		set[xxhash.Sum64String(s[start:])] = struct{}{}
	}
}

// AddTrigramHashes inserts xxhash64 keys for lowercased length-n rune n-grams of s.
func AddTrigramHashes(set map[uint64]struct{}, s string, n int) {
	if n <= 0 || s == "" {
		return
	}
	runes := []rune(strings.ToLower(s))
	if n > len(runes) {
		return
	}
	var buf [utf8.UTFMax * 8]byte
	for i := 0; i <= len(runes)-n; i++ {
		off := 0
		for _, r := range runes[i : i+n] {
			off += utf8.EncodeRune(buf[off:], r)
		}
		set[xxhash.Sum64(buf[:off])] = struct{}{}
	}
}
