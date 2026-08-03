package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Duration is a time.Duration that decodes from a Go duration string such as
// "30s" or "1m30s". The literal 0 is also accepted; bare non-zero numbers are
// rejected because their unit would be ambiguous.
type Duration time.Duration

// MarshalJSON emits a quoted Go duration string (e.g. "2s") so a config
// serialized for inspection reloads unchanged through UnmarshalJSON.
func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(time.Duration(d).String())), nil
}

// UnmarshalJSON decodes a quoted Go duration string (or the literal 0).
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "0" {
		*d = 0
		return nil
	}
	if len(s) < 2 || s[0] != '"' {
		return fmt.Errorf(`must be a quoted duration like "30s", got %s`, s)
	}
	unq, err := strconv.Unquote(s)
	if err != nil {
		return fmt.Errorf("duration: %w", err)
	}
	parsed, err := time.ParseDuration(unq)
	if err != nil {
		return fmt.Errorf("duration %q: %w", unq, err)
	}
	*d = Duration(parsed)
	return nil
}

// ByteSize is a byte quantity that decodes from a human string ("12MiB",
// "1KiB", "1MB") or a plain byte count (number or quoted digits). Binary units
// (KiB/MiB/GiB) are powers of 1024; SI units (KB/MB/GB) are powers of 1000.
type ByteSize int64

// MarshalJSON emits the plain byte count, which reloads unchanged through
// UnmarshalJSON's numeric path (no unit ambiguity).
func (s ByteSize) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(s), 10)), nil
}

// UnmarshalJSON decodes a byte quantity from a string or a plain number.
func (s *ByteSize) UnmarshalJSON(b []byte) error {
	str := strings.TrimSpace(string(b))
	if str == "" {
		return fmt.Errorf("byte size: empty")
	}
	if str[0] != '"' {
		n, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return fmt.Errorf("byte size %s: %w", str, err)
		}
		if n < 0 {
			return fmt.Errorf("byte size %s: must be >= 0", str)
		}
		*s = ByteSize(n)
		return nil
	}
	unq, err := strconv.Unquote(str)
	if err != nil {
		return fmt.Errorf("byte size: %w", err)
	}
	n, err := parseByteSize(unq)
	if err != nil {
		return err
	}
	*s = ByteSize(n)
	return nil
}

// parseByteSize parses a number followed by an optional size unit.
func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("byte size: empty")
	}
	i := 0
	for i < len(s) && (s[i] == '.' || s[i] == '-' || s[i] == '+' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	numPart := s[:i]
	unit := strings.TrimSpace(strings.ToLower(s[i:]))
	if numPart == "" {
		return 0, fmt.Errorf("byte size %q: missing number", s)
	}
	val, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("byte size %q: %w", s, err)
	}
	if val < 0 {
		return 0, fmt.Errorf("byte size %q: must be >= 0", s)
	}
	var mult float64
	switch unit {
	case "", "b":
		mult = 1
	case "k", "kb":
		mult = 1e3
	case "ki", "kib":
		mult = 1 << 10
	case "m", "mb":
		mult = 1e6
	case "mi", "mib":
		mult = 1 << 20
	case "g", "gb":
		mult = 1e9
	case "gi", "gib":
		mult = 1 << 30
	default:
		return 0, fmt.Errorf("byte size %q: unknown unit %q", s, unit)
	}
	return int64(val * mult), nil
}
