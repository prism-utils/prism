package query

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const hotSnapshotPinPrefix = ".read-"

func pinHotSnapshotSources(sources []metricsSource) ([]string, error) {
	pins := make([]string, 0, 2)
	for i := range sources {
		base := filepath.Base(sources[i].Path)
		if base != "current.parquet" && base != "current.duckdb" {
			continue
		}
		pin, err := pinHotSnapshotFile(sources[i].Path)
		if err != nil {
			unlinkPins(pins)
			return nil, err
		}
		sources[i].Path = pin
		pins = append(pins, pin)
	}
	return pins, nil
}

// pinHotSnapshotFile gives the sandbox a unique sibling of the published hot
// snapshot. The writer atomically replaces that published name; DuckDB binds
// parquet footers by path, so a rename mid-scan would otherwise mix footer
// offsets with a new, often empty, file. Hardlink keeps the inode with no extra
// bytes; copy is only the cross-device fallback.
func pinHotSnapshotFile(src string) (string, error) {
	dir := filepath.Dir(src)
	ext := filepath.Ext(src)
	for range 8 {
		var buf [8]byte
		if _, err := rand.Read(buf[:]); err != nil {
			return "", fmt.Errorf("query: pin id: %w", err)
		}
		pin := filepath.Join(dir, hotSnapshotPinPrefix+hex.EncodeToString(buf[:])+ext)
		err := os.Link(src, pin)
		if err == nil {
			return pin, nil
		}
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if isEXDEV(err) {
			if err := copyHotSnapshotFile(src, pin); err != nil {
				removePin(pin)
				return "", err
			}
			return pin, nil
		}
		return "", fmt.Errorf("query: pin hot snapshot: %w", err)
	}
	return "", fmt.Errorf("query: pin hot snapshot: name collision")
}

func isEXDEV(err error) bool {
	return errors.Is(err, syscall.EXDEV)
}

func copyHotSnapshotFile(src, dst string) error {
	in, err := os.Open(src) //nolint:gosec // G703: src is a tenant-validated hot snapshot path
	if err != nil {
		return fmt.Errorf("query: copy hot snapshot: %w", err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G703: dst is a unique sibling under the tenant hot dir
	if err != nil {
		return fmt.Errorf("query: copy hot snapshot: %w", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		removePin(dst)
		return fmt.Errorf("query: copy hot snapshot: %w", err)
	}
	if err := out.Close(); err != nil {
		removePin(dst)
		return fmt.Errorf("query: copy hot snapshot: %w", err)
	}
	return nil
}

func unlinkPins(pins []string) {
	for _, p := range pins {
		removePin(p)
	}
}

func removePin(path string) {
	_ = os.Remove(path) //nolint:gosec // G703: path is a unique sibling under the tenant hot dir
}

func withPinnedCleanup(cleanup func(), pins []string) func() {
	return func() {
		unlinkPins(pins)
		cleanup()
	}
}
