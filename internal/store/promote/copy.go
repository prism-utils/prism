package promote

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/prism-utils/prism/internal/duckdbfile"
	"github.com/prism-utils/prism/internal/store/layout"
)

const parquetMagic = "PAR1"

// CopyAtomic copies src to dest on dest's filesystem: unique temp, full copy,
// fsync, checksum, payload-magic check for .parquet/.duckdb, then rename. The source
// is never removed here. A failure leaves dest unpublished (temp removed).
func CopyAtomic(src, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o750); err != nil {
		return fmt.Errorf("promote: mkdir dest: %w", err)
	}
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		return fmt.Errorf("promote: tmp id: %w", err)
	}
	tmp := layout.PromoteTempPath(filepath.Dir(dest), filepath.Base(dest), id[:])
	if err := copyAndFsync(src, tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	srcSum, err := sha256File(src)
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	dstSum, err := sha256File(tmp)
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if srcSum != dstSum {
		_ = os.Remove(tmp)
		return fmt.Errorf("promote: checksum mismatch")
	}
	if filepath.Ext(dest) == ".parquet" {
		if err := verifyParquetMagic(tmp); err != nil {
			_ = os.Remove(tmp)
			return err
		}
	}
	if filepath.Ext(dest) == ".duckdb" {
		if err := verifyDuckDBMagic(tmp); err != nil {
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("promote: rename: %w", err)
	}
	if err := fsyncDir(filepath.Dir(dest)); err != nil {
		return fmt.Errorf("promote: fsync dest dir: %w", err)
	}
	return nil
}

func copyAndFsync(src, dst string) error {
	in, err := os.Open(src) //nolint:gosec // G304: src is a server-owned segment path
	if err != nil {
		return fmt.Errorf("promote: open source: %w", err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G304: dst is a unique temp beside dest
	if err != nil {
		return fmt.Errorf("promote: create temp: %w", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("promote: copy: %w", err)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return fmt.Errorf("promote: fsync temp: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("promote: close temp: %w", err)
	}
	return nil
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path is a server-owned segment or temp
	if err != nil {
		return "", fmt.Errorf("promote: hash open: %w", err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("promote: hash: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func verifyParquetMagic(path string) error {
	f, err := os.Open(path) //nolint:gosec // G304: path is the promote temp
	if err != nil {
		return fmt.Errorf("promote: parquet open: %w", err)
	}
	defer func() { _ = f.Close() }()
	head := make([]byte, 4)
	if _, err := io.ReadFull(f, head); err != nil {
		return fmt.Errorf("promote: parquet header: %w", err)
	}
	if string(head) != parquetMagic {
		return fmt.Errorf("promote: parquet magic")
	}
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("promote: parquet stat: %w", err)
	}
	if st.Size() < 8 {
		return fmt.Errorf("promote: parquet truncated")
	}
	if _, err := f.Seek(-4, io.SeekEnd); err != nil {
		return fmt.Errorf("promote: parquet seek: %w", err)
	}
	tail := make([]byte, 4)
	if _, err := io.ReadFull(f, tail); err != nil {
		return fmt.Errorf("promote: parquet footer: %w", err)
	}
	if string(tail) != parquetMagic {
		return fmt.Errorf("promote: parquet footer magic")
	}
	return nil
}

func verifyDuckDBMagic(path string) error {
	f, err := os.Open(path) //nolint:gosec // G304: path is the promote temp
	if err != nil {
		return fmt.Errorf("promote: duckdb open: %w", err)
	}
	defer func() { _ = f.Close() }()
	head := make([]byte, duckdbfile.MagicPeek)
	n, err := io.ReadFull(f, head)
	if err != nil && n < duckdbfile.MagicPeek {
		return fmt.Errorf("promote: duckdb header: %w", err)
	}
	if !duckdbfile.HasMagic(head[:n]) {
		return fmt.Errorf("promote: duckdb magic")
	}
	return nil
}

func fsyncDir(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // G304: dir is the dest parent
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	err = f.Sync()
	if err != nil && (errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP)) {
		return nil
	}
	return err
}
