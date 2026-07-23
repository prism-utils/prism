package cluster

import (
	"errors"
	"fmt"
	"strings"
)

// Mode is the prism-store deployment role.
type Mode string

// Deployment mode constants for MODE bootstrap config.
const (
	ModeStandalone Mode = "standalone"
	ModeClient     Mode = "client"
	ModeCluster    Mode = "cluster"
)

// ErrInvalidMode is returned when MODE is not a recognized value.
var ErrInvalidMode = errors.New("invalid mode")

// ParseMode reads the MODE bootstrap value. Empty defaults to standalone.
func ParseMode(raw string) (Mode, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ModeStandalone, nil
	}
	switch Mode(raw) {
	case ModeStandalone, ModeClient, ModeCluster:
		return Mode(raw), nil
	default:
		return "", fmt.Errorf("MODE %q: %w", raw, ErrInvalidMode)
	}
}
