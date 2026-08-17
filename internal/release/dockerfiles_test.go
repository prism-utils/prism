package release_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// patchedLibblkidContract lists contract clauses missing from a Dockerfile body.
// A next util-linux CVE bump should fail here until the pin is updated.
func patchedLibblkidContract(body string) []string {
	flat := strings.ReplaceAll(body, "\\\n", " ")
	var missing []string
	if !regexp.MustCompile(`apt-get install[^\n]*libblkid1`).MatchString(flat) {
		missing = append(missing, "apt-get install libblkid1")
	}
	if !strings.Contains(flat, "apt-get upgrade") {
		missing = append(missing, "apt-get upgrade")
	}
	if !strings.Contains(body, "trixie-security") {
		missing = append(missing, "trixie-security")
	}
	if !strings.Contains(body, "2.41.5-0+deb13u1") {
		missing = append(missing, "2.41.5-0+deb13u1")
	}
	return missing
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller")
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func TestPatchedLibblkidContract_edgeCases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		body    string
		missing []string
	}{
		{
			name:    "empty",
			body:    "",
			missing: []string{"apt-get install libblkid1", "apt-get upgrade", "trixie-security", "2.41.5-0+deb13u1"},
		},
		{
			name:    "comment only",
			body:    "# libblkid1 2.41.5-0+deb13u1 from trixie-security\nFROM debian:trixie-slim\n",
			missing: []string{"apt-get install libblkid1", "apt-get upgrade"},
		},
		{
			name:    "install without security pin",
			body:    "RUN apt-get update \\\n && apt-get install -y --no-install-recommends libblkid1 \\\n && apt-get upgrade -y\n",
			missing: []string{"trixie-security", "2.41.5-0+deb13u1"},
		},
		{
			name: "happy continued RUN",
			body: "# libblkid1 2.41-5 is CVE-2026-53615 HIGH; trixie-security ships 2.41.5-0+deb13u1.\n" +
				"RUN apt-get update \\\n && apt-get install -y --no-install-recommends ca-certificates libstdc++6 libblkid1 \\\n && apt-get upgrade -y \\\n && rm -rf /var/lib/apt/lists/*\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.missing, patchedLibblkidContract(tc.body))
		})
	}
}

func TestReleaseDockerfilesUpgradeLibblkidFromTrixieSecurity(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	for _, name := range []string{"Dockerfile.release", "Dockerfile.store.release"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			body, err := os.ReadFile(filepath.Join(root, name))
			require.NoError(t, err)
			require.NotEmpty(t, body)
			require.Empty(t, patchedLibblkidContract(string(body)), "%s must install patched libblkid from trixie-security", name)
			require.Contains(t, string(body), "debian:trixie-slim")
		})
	}
}

func TestAlertReleaseDockerfileStaysDistroless(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile(filepath.Join(repoRoot(t), "Dockerfile.alert.release"))
	require.NoError(t, err)
	text := string(body)
	require.Contains(t, text, "distroless")
	require.NotContains(t, text, "apt-get")
	require.NotContains(t, text, "libblkid1")
}
