package materialize

import (
	"errors"
	"strings"
)

var (
	errEmptySQL       = errors.New("empty sql")
	errNonSelect      = errors.New("non-select sql")
	errMultiStatement = errors.New("multi-statement sql")
)

func validateReadOnlySQL(raw string) error {
	s := stripSQLComments(strings.TrimSpace(raw))
	if s == "" {
		return errEmptySQL
	}
	s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), ";"))
	if strings.Contains(s, ";") {
		return errMultiStatement
	}
	s = stripStringLiterals(s)
	s = strings.TrimSpace(s)
	if s == "" {
		return errEmptySQL
	}
	if containsForbiddenKeyword(s) {
		return errNonSelect
	}
	upper := strings.ToUpper(s)
	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return nil
	case strings.HasPrefix(upper, "WITH"):
		return nil
	default:
		return errNonSelect
	}
}

var forbiddenSQLKeywords = []string{
	"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "ATTACH",
	"COPY", "INSTALL", "LOAD", "PRAGMA", "EXPORT", "CALL", "SET", "RESET",
}

func containsForbiddenKeyword(s string) bool {
	upper := strings.ToUpper(s)
	for _, kw := range forbiddenSQLKeywords {
		if keywordAt(upper, kw) {
			return true
		}
	}
	return false
}

func keywordAt(upper, kw string) bool {
	i := 0
	for {
		j := strings.Index(upper[i:], kw)
		if j < 0 {
			return false
		}
		j += i
		beforeOK := j == 0 || !isIdentByte(upper[j-1])
		after := j + len(kw)
		afterOK := after == len(upper) || !isIdentByte(upper[after])
		if beforeOK && afterOK {
			return true
		}
		i = j + len(kw)
	}
}

func isIdentByte(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

func stripSQLComments(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inStr := false
	for i := 0; i < len(s); i++ {
		if inStr {
			b.WriteByte(s[i])
			if s[i] == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					b.WriteByte(s[i+1])
					i++
					continue
				}
				inStr = false
			}
			continue
		}
		if s[i] == '\'' {
			inStr = true
			b.WriteByte(s[i])
			continue
		}
		if s[i] == '-' && i+1 < len(s) && s[i+1] == '-' {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			if i < len(s) {
				b.WriteByte('\n')
			}
			continue
		}
		if s[i] == '/' && i+1 < len(s) && s[i+1] == '*' {
			i += 2
			for i+1 < len(s) && (s[i] != '*' || s[i+1] != '/') {
				i++
			}
			i++
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func stripStringLiterals(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inStr := false
	for i := 0; i < len(s); i++ {
		if inStr {
			if s[i] == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inStr = false
			}
			continue
		}
		if s[i] == '\'' {
			inStr = true
			b.WriteByte(' ')
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
