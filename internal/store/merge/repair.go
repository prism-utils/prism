package merge

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/segformat"
)

// RepairLogSegmentExtensions renames live log segments whose extension does not
// match payload magic and clears skip sidecars that blocked those files.
func RepairLogSegmentExtensions(dataDir, tenant string) (int, error) {
	artifacts, err := ListLogArtifacts(dataDir, tenant)
	if err != nil {
		return 0, err
	}
	var n int
	for _, artifact := range artifacts {
		dirs := []string{layout.LogsLandingDir(dataDir, tenant, artifact)}
		for tier := 0; tier <= 8; tier++ {
			dirs = append(dirs, layout.LogsTierDir(dataDir, tenant, artifact, tier))
		}
		for _, dir := range dirs {
			k, err := repairLogDir(dir)
			if err != nil {
				return n, err
			}
			n += k
		}
	}
	return n, nil
}

func repairLogDir(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var n int
	for _, e := range entries {
		if e.IsDir() || !isSegmentFile(e.Name()) {
			continue
		}
		path := filepath.Join(dir, e.Name())
		payload := segformat.Payload(path)
		if payload == "" {
			continue
		}
		wantExt := payload.DotExt()
		gotExt := filepath.Ext(path)
		if gotExt == wantExt {
			_ = clearFormatSkip(path)
			continue
		}
		dest := strings.TrimSuffix(path, gotExt) + wantExt
		if _, err := os.Stat(dest); err == nil {
			continue
		}
		if err := os.Rename(path, dest); err != nil {
			return n, err
		}
		_ = os.Remove(layout.MergeSkipMarker(path))
		_ = os.Remove(layout.MergeAttemptsMarker(path))
		_ = os.Remove(layout.MergeSkipMarker(dest))
		_ = os.Remove(layout.MergeAttemptsMarker(dest))
		n++
	}
	return n, nil
}

func clearFormatSkip(path string) error {
	marker := layout.MergeSkipMarker(path)
	body, err := os.ReadFile(marker) //nolint:gosec // G304: sidecar beside a server-owned segment
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	reason := skipReasonFromBody(string(body))
	if reason != skipReasonTooLarge && reason != "format-mismatch" && reason != "mixed-format" {
		return nil
	}
	_ = os.Remove(marker)
	_ = os.Remove(layout.MergeAttemptsMarker(path))
	return nil
}

func skipReasonFromBody(body string) string {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "reason=") {
			return strings.TrimPrefix(line, "reason=")
		}
	}
	return ""
}
