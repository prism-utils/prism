package query

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// errUnsupportedLogQL marks a query that is valid LogQL but outside the subset
// this store implements: metric expressions (rate, count_over_time, aggregations)
// and pipeline stages beyond line filters (parsers, label filters, formatters).
// Callers translate it into a 400 with the message, so a user sees a precise
// "not supported here" instead of a silently wrong result.
var errUnsupportedLogQL = errors.New("unsupported LogQL: this store implements stream selectors with line filters only (no metric queries, parsers, or label filters)")

// logQLMatchOp is the comparison an operator performs. The same four operators
// spell both label matchers (`=`, `!=`, `=~`, `!~`) and line filters (`|=`, `!=`,
// `|~`, `!~`), so one type serves both.
type logQLMatchOp int

const (
	logQLEqual logQLMatchOp = iota
	logQLNotEqual
	logQLMatchRegex
	logQLNotMatchRegex
)

// logQLMatcher constrains one stream label. Regex operators are fully anchored,
// the way LogQL and PromQL both define label matching.
type logQLMatcher struct {
	label string
	op    logQLMatchOp
	value string
	re    *regexp.Regexp
}

// logQLLineFilter constrains the log line itself. Regex operators match a
// substring (unanchored), which is what LogQL's `|~` means.
type logQLLineFilter struct {
	op    logQLMatchOp
	value string
	re    *regexp.Regexp
}

// logQLQuery is a parsed query from the supported subset. No matchers and no
// filters means "match everything" — the friendly reading of an empty query.
type logQLQuery struct {
	matchers []logQLMatcher
	filters  []logQLLineFilter
}

// newLogQLMatcher builds a label matcher, compiling and anchoring the pattern for
// the regex operators so an invalid pattern is rejected before any query runs.
func newLogQLMatcher(label string, op logQLMatchOp, value string) (logQLMatcher, error) {
	m := logQLMatcher{label: label, op: op, value: value}
	if op == logQLMatchRegex || op == logQLNotMatchRegex {
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return logQLMatcher{}, fmt.Errorf("invalid regex %q for label %q: %w", value, label, err)
		}
		m.re = re
	}
	return m, nil
}

// matches reports whether a label value satisfies the matcher. An absent label is
// the empty string, so `!=`/`!~` matchers select streams lacking the label —
// the LogQL semantics for a missing label.
func (m logQLMatcher) matches(value string) bool {
	switch m.op {
	case logQLEqual:
		return value == m.value
	case logQLNotEqual:
		return value != m.value
	case logQLMatchRegex:
		return m.re.MatchString(value)
	case logQLNotMatchRegex:
		return !m.re.MatchString(value)
	default:
		return false
	}
}

func newLogQLLineFilter(op logQLMatchOp, value string) (logQLLineFilter, error) {
	f := logQLLineFilter{op: op, value: value}
	if op == logQLMatchRegex || op == logQLNotMatchRegex {
		re, err := regexp.Compile(value)
		if err != nil {
			return logQLLineFilter{}, fmt.Errorf("invalid line filter regex %q: %w", value, err)
		}
		f.re = re
	}
	return f, nil
}

// parseLogQL parses the supported LogQL subset: an optional stream selector
// followed by any number of line filters. An empty expression, or a selector with
// no matchers, is a match-all query so a bare Explore query still returns logs.
// Anything richer returns errUnsupportedLogQL; anything malformed returns a
// parse error naming the offending position.
func parseLogQL(expr string) (*logQLQuery, error) {
	s := strings.TrimSpace(expr)
	q := &logQLQuery{}
	if s == "" {
		return q, nil
	}
	if s[0] != '{' {
		return nil, errUnsupportedLogQL
	}
	rest, err := parseLogQLSelector(s, q)
	if err != nil {
		return nil, err
	}
	if err := parseLogQLFilters(rest, q); err != nil {
		return nil, err
	}
	return q, nil
}

// parseLogQLSelector consumes the leading `{...}` and returns the remainder.
func parseLogQLSelector(s string, q *logQLQuery) (string, error) {
	i := 1
	for {
		i = skipLogQLSpace(s, i)
		if i < len(s) && s[i] == '}' {
			return s[i+1:], nil
		}
		if len(q.matchers) > 0 {
			if i >= len(s) || s[i] != ',' {
				return "", fmt.Errorf("logql: expected ',' or '}' at position %d", i)
			}
			i = skipLogQLSpace(s, i+1)
		}
		label, next, err := parseLogQLLabelName(s, i)
		if err != nil {
			return "", err
		}
		i = skipLogQLSpace(s, next)
		op, next, err := parseLogQLOperator(s, i, false)
		if err != nil {
			return "", err
		}
		i = skipLogQLSpace(s, next)
		value, next, err := parseLogQLString(s, i)
		if err != nil {
			return "", err
		}
		i = next
		m, err := newLogQLMatcher(label, op, value)
		if err != nil {
			return "", fmt.Errorf("logql: %w", err)
		}
		q.matchers = append(q.matchers, m)
	}
}

// parseLogQLFilters consumes the line-filter pipeline that may follow a selector.
// Any other pipeline stage or a range/vector suffix is out of the subset.
func parseLogQLFilters(s string, q *logQLQuery) error {
	i := skipLogQLSpace(s, 0)
	for i < len(s) {
		op, next, err := parseLogQLOperator(s, i, true)
		if err != nil {
			return err
		}
		i = skipLogQLSpace(s, next)
		value, next, err := parseLogQLString(s, i)
		if err != nil {
			return err
		}
		i = skipLogQLSpace(s, next)
		f, err := newLogQLLineFilter(op, value)
		if err != nil {
			return fmt.Errorf("logql: %w", err)
		}
		q.filters = append(q.filters, f)
	}
	return nil
}

func parseLogQLLabelName(s string, i int) (string, int, error) {
	start := i
	for i < len(s) && isLogQLLabelChar(s[i], i == start) {
		i++
	}
	if i == start {
		return "", 0, fmt.Errorf("logql: expected a label name at position %d", start)
	}
	name := s[start:i]
	if !isValidLabelName(name) {
		return "", 0, fmt.Errorf("logql: invalid label name %q", name)
	}
	return name, i, nil
}

// parseLogQLOperator reads a comparison operator. In line-filter position the
// contains operator is spelled `|=` and the regex operator `|~`; a lone `|`
// introduces a pipeline stage this store does not implement.
func parseLogQLOperator(s string, i int, lineFilter bool) (logQLMatchOp, int, error) {
	if i >= len(s) {
		return 0, 0, fmt.Errorf("logql: expected an operator at position %d", i)
	}
	two := ""
	if i+1 < len(s) {
		two = s[i : i+2]
	}
	switch {
	case two == "!=":
		return logQLNotEqual, i + 2, nil
	case two == "!~":
		return logQLNotMatchRegex, i + 2, nil
	case lineFilter && two == "|=":
		return logQLEqual, i + 2, nil
	case lineFilter && two == "|~":
		return logQLMatchRegex, i + 2, nil
	case !lineFilter && two == "=~":
		return logQLMatchRegex, i + 2, nil
	case !lineFilter && s[i] == '=':
		return logQLEqual, i + 1, nil
	case lineFilter:
		// `| json`, `| label_format`, `[5m]`, arithmetic — valid LogQL, not here.
		return 0, 0, errUnsupportedLogQL
	default:
		return 0, 0, fmt.Errorf("logql: expected one of =, !=, =~, !~ at position %d", i)
	}
}

// parseLogQLString reads a double-quoted (escapes interpreted) or backtick-quoted
// (raw) string literal, the two spellings LogQL accepts.
func parseLogQLString(s string, i int) (string, int, error) {
	if i >= len(s) {
		return "", 0, fmt.Errorf("logql: expected a quoted value at position %d", i)
	}
	switch s[i] {
	case '"':
		end := i + 1
		for end < len(s) {
			if s[end] == '\\' {
				end += 2
				continue
			}
			if s[end] == '"' {
				break
			}
			end++
		}
		if end >= len(s) {
			return "", 0, fmt.Errorf("logql: unterminated string at position %d", i)
		}
		v, err := strconv.Unquote(s[i : end+1])
		if err != nil {
			return "", 0, fmt.Errorf("logql: invalid string at position %d: %w", i, err)
		}
		return v, end + 1, nil
	case '`':
		end := strings.IndexByte(s[i+1:], '`')
		if end < 0 {
			return "", 0, fmt.Errorf("logql: unterminated string at position %d", i)
		}
		return s[i+1 : i+1+end], i + end + 2, nil
	default:
		return "", 0, fmt.Errorf("logql: expected a quoted value at position %d", i)
	}
}

func skipLogQLSpace(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

func isLogQLLabelChar(b byte, first bool) bool {
	switch {
	case b == '_':
		return true
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z':
		return true
	case b >= '0' && b <= '9':
		return !first
	default:
		return false
	}
}
