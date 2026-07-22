package bloom

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cespare/xxhash/v2"
)

// Filter is a classic Bloom filter using Kirsch–Mitzenmacher double hashing.
//
// Uses a 64-bit xxhash split into two 32-bit halves (h1|h2) to derive k bit
// positions via g_i = h1 + i·h2 mod m. A query checks that all k positions are set.
type Filter struct {
	m      int
	k      int
	nItems int
	bits   []byte
}

// BuildFromHashes constructs a filter from distinct item hashes (xxhash64 of each item).
func BuildFromHashes(hashes map[uint64]struct{}, fp float64) *Filter {
	n := len(hashes)
	if n == 0 {
		return &Filter{m: 1, k: 1, nItems: 0, bits: make([]byte, 1)}
	}
	ln2 := math.Ln2
	m := int(math.Ceil(-float64(n) * math.Log(fp) / (ln2 * ln2)))
	if m < 1 {
		m = 1
	}
	k := int(math.Round(float64(m) / float64(n) * ln2))
	if k < 1 {
		k = 1
	}
	bitBytes := (m + 7) / 8
	f := &Filter{m: m, k: k, nItems: n, bits: make([]byte, bitBytes)}
	for h := range hashes {
		f.addHash(h)
	}
	return f
}

// BuildFromSet constructs a filter from a set of distinct non-empty strings.
func BuildFromSet(set map[string]struct{}, fp float64) *Filter {
	if len(set) == 0 {
		return BuildDistinct(nil, fp)
	}
	n := len(set)
	ln2 := math.Ln2
	m := int(math.Ceil(-float64(n) * math.Log(fp) / (ln2 * ln2)))
	if m < 1 {
		m = 1
	}
	k := int(math.Round(float64(m) / float64(n) * ln2))
	if k < 1 {
		k = 1
	}
	bitBytes := (m + 7) / 8
	f := &Filter{m: m, k: k, nItems: n, bits: make([]byte, bitBytes)}
	for item := range set {
		if item != "" {
			f.add(item)
		}
	}
	return f
}

// BuildDistinct constructs a Bloom filter from already-distinct non-empty items.
func BuildDistinct(items []string, fp float64) *Filter {
	n := 0
	for _, item := range items {
		if item != "" {
			n++
		}
	}
	if n == 0 {
		return &Filter{m: 1, k: 1, nItems: 0, bits: make([]byte, 1)}
	}

	ln2 := math.Ln2
	m := int(math.Ceil(-float64(n) * math.Log(fp) / (ln2 * ln2)))
	if m < 1 {
		m = 1
	}
	k := int(math.Round(float64(m) / float64(n) * ln2))
	if k < 1 {
		k = 1
	}

	bitBytes := (m + 7) / 8
	f := &Filter{m: m, k: k, nItems: n, bits: make([]byte, bitBytes)}
	for _, item := range items {
		if item != "" {
			f.add(item)
		}
	}
	return f
}

// Build constructs a Bloom filter for the given items at false-positive target fp.
// Duplicate items are deduplicated. An empty item set yields a tiny valid filter
// that answers Contains=false for every query.
func Build(items []string, fp float64) *Filter {
	uniq := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item == "" {
			continue
		}
		uniq[item] = struct{}{}
	}
	if len(uniq) == 0 {
		return BuildDistinct(nil, fp)
	}
	distinct := make([]string, 0, len(uniq))
	for item := range uniq {
		distinct = append(distinct, item)
	}
	return BuildDistinct(distinct, fp)
}

func (f *Filter) addHash(seed uint64) {
	h1 := uint32(seed >> 32) //nolint:gosec // G115: intentional low/high split for double hashing
	h2 := uint32(seed)       //nolint:gosec // G115: intentional low/high split for double hashing
	for i := 0; i < f.k; i++ {
		idx := int((uint64(h1) + uint64(i)*uint64(h2)) % uint64(f.m)) //nolint:gosec // G115: m is positive
		f.bits[idx/8] |= 1 << (idx % 8)
	}
}

func (f *Filter) add(item string) {
	f.addHash(xxhash.Sum64String(item))
}

// Contains reports whether item might be in the set (never a false negative).
func (f *Filter) Contains(item string) bool {
	if f.nItems == 0 {
		return false
	}
	h := xxhash.Sum64String(item)
	h1 := uint32(h >> 32) //nolint:gosec // G115: intentional low/high split for double hashing
	h2 := uint32(h)       //nolint:gosec // G115: intentional low/high split for double hashing
	for i := 0; i < f.k; i++ {
		idx := int((uint64(h1) + uint64(i)*uint64(h2)) % uint64(f.m)) //nolint:gosec // G115: m is positive
		if f.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

// Marshal encodes the filter as a base64 blob: a 10-byte header
// {m uint32 LE, k uint16 LE, n_items uint32 LE} followed by ceil(m/8) bitset bytes.
func (f *Filter) Marshal() (string, error) {
	raw := make([]byte, 10, 10+len(f.bits))
	binary.LittleEndian.PutUint32(raw[0:4], uint32(f.m))       //nolint:gosec // G115: m sized from int formula
	binary.LittleEndian.PutUint16(raw[4:6], uint16(f.k))       //nolint:gosec // G115: k is small (typical <20)
	binary.LittleEndian.PutUint32(raw[6:10], uint32(f.nItems)) //nolint:gosec // G115: n_items is row-group distinct count
	raw = append(raw, f.bits...)
	return base64.StdEncoding.EncodeToString(raw), nil
}

// Unmarshal decodes a base64 blob produced by Marshal.
func Unmarshal(blob string) (*Filter, error) {
	raw, err := base64.StdEncoding.DecodeString(blob)
	if err != nil {
		return nil, fmt.Errorf("bloom: decode base64: %w", err)
	}
	if len(raw) < 10 {
		return nil, fmt.Errorf("bloom: blob too short")
	}
	m := int(binary.LittleEndian.Uint32(raw[0:4]))
	k := int(binary.LittleEndian.Uint16(raw[4:6]))
	nItems := int(binary.LittleEndian.Uint32(raw[6:10]))
	wantBits := (m + 7) / 8
	if len(raw) != 10+wantBits {
		return nil, fmt.Errorf("bloom: bitset length mismatch")
	}
	bits := make([]byte, wantBits)
	copy(bits, raw[10:])
	return &Filter{m: m, k: k, nItems: nItems, bits: bits}, nil
}
