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

type hotPins struct {
	paths    []string
	extraDir string
}

func (p hotPins) cleanup() {
	unlinkPins(p.paths)
	if p.extraDir != "" {
		_ = os.RemoveAll(p.extraDir) //nolint:gosec // G703: extraDir is a private MkdirTemp the sandbox created
	}
}

func pinHotSnapshotSources(sources []metricsSource) (hotPins, error) {
	out := hotPins{paths: make([]string, 0, 2)}
	for i := range sources {
		base := filepath.Base(sources[i].Path)
		if base != "current.parquet" && base != "current.duckdb" {
			continue
		}
		pin, err := pinHotSnapshotFile(sources[i].Path, &out.extraDir)
		if err != nil {
			out.cleanup()
			return hotPins{}, err
		}
		sources[i].Path = pin
		out.paths = append(out.paths, pin)
	}
	return out, nil
}

// pinHotSnapshotFile gives the sandbox a path that export rename cannot
// clobber. DuckDB binds parquet footers by path, so a replace mid-scan would
// otherwise mix footer offsets with a new, often empty, file. Hardlink to a
// unique sibling keeps the inode with no extra bytes; copy is the cross-device
// fallback. A read-only tenant directory cannot receive a sibling, so the pin
// is copied into a private temporary directory instead.
func pinHotSnapshotFile(src string, extraDir *string) (string, error) {
	pin, err := linkOrCopyPin(src, filepath.Dir(src))
	if err == nil {
		return pin, nil
	}
	if !isEXDEV(err) && !isNotWritable(err) {
		return "", fmt.Errorf("query: pin hot snapshot: %w", err)
	}
	if isEXDEV(err) {
		pin, copyErr := copyPinBeside(src, filepath.Dir(src))
		if copyErr == nil {
			return pin, nil
		}
		if !isNotWritable(copyErr) {
			return "", copyErr
		}
	}
	return copyPinToTemp(src, extraDir)
}

func linkOrCopyPin(src, dir string) (string, error) {
	ext := filepath.Ext(src)
	for range 8 {
		pin, err := uniquePinPath(dir, ext)
		if err != nil {
			return "", err
		}
		err = os.Link(src, pin)
		if err == nil {
			return pin, nil
		}
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return "", err
	}
	return "", fmt.Errorf("query: pin hot snapshot: name collision")
}

func copyPinBeside(src, dir string) (string, error) {
	ext := filepath.Ext(src)
	for range 8 {
		pin, err := uniquePinPath(dir, ext)
		if err != nil {
			return "", err
		}
		err = copyHotSnapshotFile(src, pin)
		if err == nil {
			return pin, nil
		}
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return "", err
	}
	return "", fmt.Errorf("query: pin hot snapshot: name collision")
}

func copyPinToTemp(src string, extraDir *string) (string, error) {
	if extraDir == nil {
		return "", fmt.Errorf("query: pin hot snapshot: missing temp dir")
	}
	if *extraDir == "" {
		d, err := os.MkdirTemp("", "prism-hotpin-")
		if err != nil {
			return "", fmt.Errorf("query: pin temp: %w", err)
		}
		*extraDir = d
	}
	pin, err := uniquePinPath(*extraDir, filepath.Ext(src))
	if err != nil {
		return "", err
	}
	if err := copyHotSnapshotFile(src, pin); err != nil {
		return "", err
	}
	return pin, nil
}

func uniquePinPath(dir, ext string) (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("query: pin id: %w", err)
	}
	return filepath.Join(dir, hotSnapshotPinPrefix+hex.EncodeToString(buf[:])+ext), nil
}

func isEXDEV(err error) bool {
	return errors.Is(err, syscall.EXDEV)
}

func isNotWritable(err error) bool {
	return errors.Is(err, syscall.EROFS) ||
		errors.Is(err, syscall.EACCES) ||
		errors.Is(err, syscall.EPERM) ||
		errors.Is(err, os.ErrPermission)
}

func copyHotSnapshotFile(src, dst string) error {
	in, err := os.Open(src) //nolint:gosec // G703: src is a tenant-validated hot snapshot path
	if err != nil {
		return fmt.Errorf("query: copy hot snapshot: %w", err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G703: dst is a unique pin path the sandbox created
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
	_ = os.Remove(path) //nolint:gosec // G703: path is a unique pin the sandbox created
}

func withPinnedCleanup(cleanup func(), pins hotPins) func() {
	return func() {
		pins.cleanup()
		cleanup()
	}
}
