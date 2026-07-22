package bloom

import "encoding/json"

// Version is the footer KV bloom schema version.
const Version = 1

// Params documents how to reconstruct membership tests for a column's blooms.
type Params struct {
	Version   int     `json:"version"`
	Hash      string  `json:"hash"`
	Combine   string  `json:"combine"`
	Tokenizer string  `json:"tokenizer"`
	NgramN    int     `json:"ngram_n,omitempty"`
	FPTarget  float64 `json:"fp_target"`
}

// DefaultParams returns the shared params JSON for a word-token bloom.
func DefaultParams(fp float64) Params {
	return Params{
		Version:   Version,
		Hash:      "xxhash64",
		Combine:   "h1+i*h2",
		Tokenizer: "word",
		FPTarget:  fp,
	}
}

// ParamsWithNgram returns params when trigram blooms are enabled.
func ParamsWithNgram(fp float64, n int) Params {
	p := DefaultParams(fp)
	p.NgramN = n
	return p
}

// MarshalParams JSON-encodes params for footer KV storage.
func MarshalParams(p Params) (string, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
