package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pipelineYAML is a minimal valid one-pipeline document, parameterized by name.
func pipelineYAML(name, target string) string {
	return `pipelines:
  - name: ` + name + `
    input: { type: prometheus, options: { targets: ["` + target + `"] } }
    parser: { type: prometheus }
    branches:
      - name: raw
        encoder: { type: parquet }
        output: { type: dir, options: { dir: /tmp/` + name + ` } }
`
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoadFile_MergesIncludedPipelines(t *testing.T) {
	dir := t.TempDir()
	main := filepath.Join(dir, "prism.yaml")
	writeFile(t, main, "include: [\"config.d/*.yaml\"]\n"+pipelineYAML("base", "http://base:9100/metrics"))
	writeFile(t, filepath.Join(dir, "config.d", "es.yaml"), pipelineYAML("elasticsearch", "http://es:9114/metrics"))
	writeFile(t, filepath.Join(dir, "config.d", "pg.yaml"), pipelineYAML("postgres", "http://pg:9187/metrics"))

	cfg, err := LoadFile(main)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	names := map[string]bool{}
	for _, p := range cfg.Pipelines {
		names[p.Name] = true
	}
	for _, want := range []string{"base", "elasticsearch", "postgres"} {
		if !names[want] {
			t.Fatalf("missing pipeline %q; got %v", want, names)
		}
	}
	if len(cfg.Pipelines) != 3 {
		t.Fatalf("pipelines = %d, want 3", len(cfg.Pipelines))
	}
}

func TestLoadFile_DuplicateNameAcrossIncludesRejected(t *testing.T) {
	dir := t.TempDir()
	main := filepath.Join(dir, "prism.yaml")
	writeFile(t, main, "include: [\"config.d/*.yaml\"]\n"+pipelineYAML("dup", "http://a/metrics"))
	writeFile(t, filepath.Join(dir, "config.d", "dup.yaml"), pipelineYAML("dup", "http://b/metrics"))

	if _, err := LoadFile(main); err == nil {
		t.Fatal("duplicate pipeline name across includes should be rejected")
	}
}

func TestLoadFile_NestedIncludeRejected(t *testing.T) {
	dir := t.TempDir()
	main := filepath.Join(dir, "prism.yaml")
	writeFile(t, main, "include: [\"config.d/*.yaml\"]\n"+pipelineYAML("base", "http://a/metrics"))
	writeFile(t, filepath.Join(dir, "config.d", "nested.yaml"),
		"include: [\"more/*.yaml\"]\n"+pipelineYAML("child", "http://b/metrics"))

	if _, err := LoadFile(main); err == nil {
		t.Fatal("an included file that itself declares include should be rejected")
	}
}

func TestLoadFile_EnvInterpolationInIncludes(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PRISM_TEST_TARGET", "http://from-env:9100/metrics")
	main := filepath.Join(dir, "prism.yaml")
	writeFile(t, main, "include: [\"config.d/*.yaml\"]\n"+pipelineYAML("base", "http://a/metrics"))
	writeFile(t, filepath.Join(dir, "config.d", "env.yaml"), pipelineYAML("enved", "${PRISM_TEST_TARGET}"))

	cfg, err := LoadFile(main)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	var found bool
	for _, p := range cfg.Pipelines {
		if p.Name == "enved" {
			found = true
		}
	}
	if !found {
		t.Fatal("env-interpolated included pipeline not loaded")
	}
}

func TestLoadFile_NoMatchesStillLoadsMain(t *testing.T) {
	dir := t.TempDir()
	main := filepath.Join(dir, "prism.yaml")
	writeFile(t, main, "include: [\"config.d/*.yaml\"]\n"+pipelineYAML("base", "http://a/metrics"))

	cfg, err := LoadFile(main)
	if err != nil {
		t.Fatalf("LoadFile with no matches: %v", err)
	}
	if len(cfg.Pipelines) != 1 {
		t.Fatalf("pipelines = %d, want 1", len(cfg.Pipelines))
	}
}

func TestLoadConfig_RejectsIncludeWithoutBaseDir(t *testing.T) {
	// The reader-based loader cannot resolve include globs (no base directory).
	_, err := LoadConfig(strings.NewReader("include: [\"config.d/*.yaml\"]\n" + pipelineYAML("base", "http://a/metrics")))
	if err == nil {
		t.Fatal("reader-based LoadConfig should reject include")
	}
}
