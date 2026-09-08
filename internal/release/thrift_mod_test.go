package release_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// patchedThriftContract returns why a go.mod body fails CVE-2026-43871's floor.
// A next Apache Thrift CVE bump should fail here until the pin is updated.
func patchedThriftContract(body string) []string {
	re := regexp.MustCompile(`(?m)^\s*(?:require\s+)?github\.com/apache/thrift\s+v(\d+)\.(\d+)\.(\d+)\b`)
	m := re.FindStringSubmatch(body)
	if m == nil {
		return []string{"github.com/apache/thrift require"}
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	if major > 0 || minor >= 24 {
		return nil
	}
	return []string{"github.com/apache/thrift >= v0.24.0"}
}

func TestPatchedThriftContract_edgeCases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		body    string
		missing []string
	}{
		{
			name:    "empty",
			body:    "",
			missing: []string{"github.com/apache/thrift require"},
		},
		{
			name:    "comment only",
			body:    "// github.com/apache/thrift v0.24.0\n",
			missing: []string{"github.com/apache/thrift require"},
		},
		{
			name:    "vulnerable 0.23.0",
			body:    "\tgithub.com/apache/thrift v0.23.0 // indirect\n",
			missing: []string{"github.com/apache/thrift >= v0.24.0"},
		},
		{
			name: "happy 0.24.0",
			body: "\tgithub.com/apache/thrift v0.24.0 // indirect\n",
		},
		{
			name: "happy 0.24.1",
			body: "require github.com/apache/thrift v0.24.1\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.missing, patchedThriftContract(tc.body))
		})
	}
}

func TestGoModPinsThriftPastCVE202643871(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile(filepath.Join(repoRoot(t), "go.mod"))
	require.NoError(t, err)
	require.NotEmpty(t, body)
	text := string(body)
	require.Empty(t, patchedThriftContract(text), "go.mod must pin apache/thrift >= v0.24.0 (CVE-2026-43871)")
	require.False(t, strings.Contains(text, "github.com/apache/thrift v0.23."), "go.mod still names thrift v0.23.x")
}
