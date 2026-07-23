// Package env collects host metadata for benchmark reproducibility notes.
package env

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// HostInfo holds CPU model and RAM for the results header.
type HostInfo struct {
	OS       string
	Arch     string
	CPUModel string
	RAMGiB   float64
}

// Collect reads OS/arch and best-effort CPU/RAM from the local machine.
func Collect() HostInfo {
	info := HostInfo{OS: runtime.GOOS, Arch: runtime.GOARCH}
	switch runtime.GOOS {
	case "darwin":
		info.CPUModel = strings.TrimSpace(run("sysctl", "-n", "machdep.cpu.brand_string"))
		if mem := run("sysctl", "-n", "hw.memsize"); mem != "" {
			if b, err := strconv.ParseInt(strings.TrimSpace(mem), 10, 64); err == nil {
				info.RAMGiB = float64(b) / (1024 * 1024 * 1024)
			}
		}
	case "linux":
		info.CPUModel = readFirstLine("/proc/cpuinfo", "model name")
		if mem := readFirstLine("/proc/meminfo", "MemTotal"); mem != "" {
			fields := strings.Fields(mem)
			if len(fields) >= 2 && fields[1] == "kB" {
				if kb, err := strconv.ParseFloat(fields[0], 64); err == nil {
					info.RAMGiB = kb / (1024 * 1024)
				}
			}
		}
	}
	if info.CPUModel == "" {
		info.CPUModel = "unknown"
	}
	return info
}

func run(name string, args ...string) string {
	//nolint:gosec // G204: fixed sysctl/git helper invocations with no user-controlled binary name.
	out, err := exec.CommandContext(context.Background(), name, args...).Output()
	if err != nil {
		return ""
	}
	return string(out)
}

func readFirstLine(path, key string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, line := range bytes.Split(b, []byte("\n")) {
		s := string(line)
		if strings.HasPrefix(s, key) {
			parts := strings.SplitN(s, ":", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return ""
}

// GitCommit returns the current HEAD sha or "unknown".
func GitCommit(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// RepoRoot finds the git root from cwd.
func RepoRoot(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("env: git root: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}
