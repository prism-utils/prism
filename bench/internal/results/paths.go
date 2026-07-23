package results

import "path/filepath"

// Artifacts holds output paths for a benchmark profile.
type Artifacts struct {
	JSON         string
	Markdown     string
	Timeseries   string
	ChartsDir    string
	ChartsPrefix string
	chartDirName string
}

// ArtifactPaths returns absolute write paths and repo-relative chart prefixes for profile
// ("" baseline, "api" RBAC JSON, "api-arrow" RBAC Arrow transport).
func ArtifactPaths(repoRoot, profile string) Artifacts {
	bench := filepath.Join(repoRoot, "bench")
	suffix := ""
	if profile != "" {
		suffix = "-" + profile
	}
	chartDirName := "charts" + suffix
	return Artifacts{
		JSON:         filepath.Join(bench, "results"+suffix+".json"),
		Markdown:     filepath.Join(bench, "RESULTS"+suffix+".md"),
		Timeseries:   filepath.Join(bench, "results-timeseries"+suffix+".json"),
		ChartsDir:    filepath.Join(bench, chartDirName),
		ChartsPrefix: filepath.ToSlash(filepath.Join("bench", chartDirName)) + "/",
		chartDirName: chartDirName,
	}
}

// ChartRel returns a repo-relative chart path for name under this profile's charts directory.
func ChartRel(a *Artifacts, name string) string {
	return filepath.ToSlash(filepath.Join("bench", a.chartDirName, name))
}
