package bloom

import "strings"

// TokenizeWords splits s on [^a-zA-Z0-9]+ and returns non-empty tokens.
func TokenizeWords(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	start := -1
	for i, r := range s {
		if isWordChar(r) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			out = append(out, s[start:i])
			start = -1
		}
	}
	if start >= 0 {
		out = append(out, s[start:])
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// AddWords tokenizes s as words and inserts them into set.
func AddWords(set map[string]struct{}, s string) {
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
			set[s[start:i]] = struct{}{}
			start = -1
		}
	}
	if start >= 0 {
		set[s[start:]] = struct{}{}
	}
}

func isWordChar(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9')
}

// TokenizeTrigrams returns lowercased length-n rune n-grams from s. When n is
// zero or greater than the rune count, it returns nil.
func TokenizeTrigrams(s string, n int) []string {
	if n <= 0 || s == "" {
		return nil
	}
	lower := strings.ToLower(s)
	runes := []rune(lower)
	if n > len(runes) {
		return nil
	}
	out := make([]string, 0, len(runes)-n+1)
	for i := 0; i <= len(runes)-n; i++ {
		out = append(out, string(runes[i:i+n]))
	}
	return out
}

// AddTrigrams tokenizes s into length-n lowercased rune n-grams in set.
func AddTrigrams(set map[string]struct{}, s string, n int) {
	if n <= 0 || s == "" {
		return
	}
	lower := strings.ToLower(s)
	runes := []rune(lower)
	if n > len(runes) {
		return
	}
	for i := 0; i <= len(runes)-n; i++ {
		set[string(runes[i:i+n])] = struct{}{}
	}
}
