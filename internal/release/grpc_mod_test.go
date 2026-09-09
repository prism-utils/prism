package release_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// patchedGRPCContract returns why a go.mod body fails CVE-2026-84445's floor.
// A next gRPC CVE bump should fail here until the pin is updated.
func patchedGRPCContract(body string) []string {
	re := regexp.MustCompile(`(?m)^\s*(?:require\s+)?google\.golang\.org/grpc\s+v(\d+)\.(\d+)\.(\d+)\b`)
	m := re.FindStringSubmatch(body)
	if m == nil {
		return []string{"google.golang.org/grpc require"}
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	patch, _ := strconv.Atoi(m[3])
	if grpcCVE202684445Patched(major, minor, patch) {
		return nil
	}
	return []string{"google.golang.org/grpc >= v1.83.2 (or v1.82.2 on the 1.82 line)"}
}

func grpcCVE202684445Patched(major, minor, patch int) bool {
	if major != 1 {
		return major > 1
	}
	if minor > 83 {
		return true
	}
	if minor == 83 {
		return patch >= 2
	}
	if minor == 82 {
		return patch >= 2
	}
	return false
}

func TestPatchedGRPCContract_edgeCases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		body    string
		missing []string
	}{
		{
			name:    "empty",
			body:    "",
			missing: []string{"google.golang.org/grpc require"},
		},
		{
			name:    "comment only",
			body:    "// google.golang.org/grpc v1.83.2\n",
			missing: []string{"google.golang.org/grpc require"},
		},
		{
			name:    "vulnerable 1.83.1",
			body:    "\tgoogle.golang.org/grpc v1.83.1\n",
			missing: []string{"google.golang.org/grpc >= v1.83.2 (or v1.82.2 on the 1.82 line)"},
		},
		{
			name:    "vulnerable 1.82.1",
			body:    "require google.golang.org/grpc v1.82.1\n",
			missing: []string{"google.golang.org/grpc >= v1.83.2 (or v1.82.2 on the 1.82 line)"},
		},
		{
			name: "happy 1.83.2",
			body: "\tgoogle.golang.org/grpc v1.83.2\n",
		},
		{
			name: "happy 1.82.2",
			body: "require google.golang.org/grpc v1.82.2\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.missing, patchedGRPCContract(tc.body))
		})
	}
}

func TestGoModPinsGRPCPastCVE202684445(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile(filepath.Join(repoRoot(t), "go.mod"))
	require.NoError(t, err)
	require.NotEmpty(t, body)
	require.Empty(t, patchedGRPCContract(string(body)), "go.mod must pin google.golang.org/grpc past CVE-2026-84445")
}
