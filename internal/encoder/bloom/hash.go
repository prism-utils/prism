package bloom

import (
	"unicode"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
)

// AddWordHashesBytes inserts xxhash64 keys for word tokens in b into set.
// b must be UTF-8; callers may pass a sub-slice of an existing buffer without copying.
func AddWordHashesBytes(set map[uint64]struct{}, b []byte) {
	if len(b) == 0 {
		return
	}
	start := -1
	for i := 0; i < len(b); i++ {
		if isWordByte(b[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			set[xxhash.Sum64(b[start:i])] = struct{}{}
			start = -1
		}
	}
	if start >= 0 {
		set[xxhash.Sum64(b[start:])] = struct{}{}
	}
}

func isWordByte(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}

// AppendLowerUTF8 lowercases UTF-8 bytes of src into dst and returns the slice.
func AppendLowerUTF8(dst, src []byte) []byte {
	dst = dst[:0]
	for len(src) > 0 {
		r, size := utf8.DecodeRune(src)
		var tmp [utf8.UTFMax]byte
		n := utf8.EncodeRune(tmp[:], unicode.ToLower(r))
		dst = append(dst, tmp[:n]...)
		src = src[size:]
	}
	return dst
}

// AddTrigramHashesBytes inserts xxhash64 keys for lowercased length-n UTF-8 rune
// n-grams from b. scratch holds the lowercased copy (reused); runeOffsets is
// reused and repopulated with the byte offset of each rune start in scratch.
func AddTrigramHashesBytes(set map[uint64]struct{}, scratch, b []byte, runeOffsets []int, n int) ([]int, []byte) {
	if n <= 0 || len(b) == 0 {
		return runeOffsets[:0], scratch
	}
	scratch = AppendLowerUTF8(scratch[:0], b)
	runeOffsets = runeOffsets[:0]
	for i := 0; i < len(scratch); {
		_, size := utf8.DecodeRune(scratch[i:])
		runeOffsets = append(runeOffsets, i)
		i += size
	}
	if n > len(runeOffsets) {
		return runeOffsets, scratch
	}
	var enc [utf8.UTFMax * 16]byte
	for i := 0; i <= len(runeOffsets)-n; i++ {
		off := 0
		for j := 0; j < n; j++ {
			start := runeOffsets[i+j]
			end := len(scratch)
			if i+j+1 < len(runeOffsets) {
				end = runeOffsets[i+j+1]
			}
			off += copy(enc[off:], scratch[start:end])
		}
		set[xxhash.Sum64(enc[:off])] = struct{}{}
	}
	return runeOffsets, scratch
}
