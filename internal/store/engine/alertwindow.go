package engine

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/prism-utils/prism/internal/store/segformat"
	storetenant "github.com/prism-utils/prism/internal/store/tenant"
)

// alertArtifactPattern admits only the alert-events ingest name so a crafted
// artifact cannot escape <tenant>/alerts/.
var alertArtifactPattern = regexp.MustCompile(`^alert-events$`)

// LandAlertWindow persists an alert-events window as an immutable parquet file
// under <tenant>/alerts/alert-events/. The write skips the metrics hot catalog
// and does not place files under logs/. Empty bodies are a no-op. Returns bytes
// written (0 for empty).
func (e *Engine) LandAlertWindow(tenant, artifact string, body io.Reader) (int64, error) {
	if !storetenant.TenantAllowed(tenant) {
		return 0, fmt.Errorf("engine: invalid tenant %q", tenant)
	}
	if !alertArtifactPattern.MatchString(artifact) {
		return 0, fmt.Errorf("engine: invalid alert artifact %q", artifact)
	}
	dir := filepath.Join(e.cfg.DataDir, tenant, "alerts", artifact)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return 0, err
	}
	tmp, err := os.CreateTemp(dir, ".window-*.parquet.tmp")
	if err != nil {
		return 0, err
	}
	tmpPath := tmp.Name()
	n, copyErr := io.Copy(tmp, body)
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return 0, mapBodyCopyErr(copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return 0, closeErr
	}
	if n == 0 {
		_ = os.Remove(tmpPath)
		return 0, nil
	}
	bodyBytes, err := os.ReadFile(tmpPath)
	_ = os.Remove(tmpPath)
	if err != nil {
		return 0, err
	}
	final := filepath.Join(dir, segmentNameFormat(e.clock(), segformat.Parquet))
	//nolint:gosec // G703: dir is tenant/alerts/<validated-artifact>; name is segmentNameFormat
	if err := os.WriteFile(final, bodyBytes, 0o600); err != nil {
		return 0, fmt.Errorf("engine: land alert window: %w", err)
	}
	return n, nil
}
