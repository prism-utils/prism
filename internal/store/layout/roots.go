package layout

import (
	"os"
	"path/filepath"
	"strings"
)

// ColdEnabled reports whether a second data root is configured.
func ColdEnabled(coldDir string) bool {
	return strings.TrimSpace(coldDir) != ""
}

// TenantDir joins a store root with a tenant namespace.
func TenantDir(root, tenant string) string {
	return filepath.Join(root, tenant)
}

// AllowedTenantRoots returns the directories a query sandbox may read. The hot
// root is always first. An empty coldDir yields a single-element list.
func AllowedTenantRoots(dataDir, coldDir, tenant string) []string {
	hot := TenantDir(dataDir, tenant)
	if !ColdEnabled(coldDir) {
		return []string{hot}
	}
	return []string{hot, TenantDir(coldDir, tenant)}
}

// ResolveRel returns the first existing file for a tenant-relative path, trying
// the hot root then the cold root. Missing on both yields ok=false.
func ResolveRel(dataDir, coldDir, tenant, rel string) (string, bool) {
	rel = filepath.FromSlash(rel)
	for _, root := range AllowedTenantRoots(dataDir, coldDir, tenant) {
		abs := filepath.Join(root, rel)
		fi, err := os.Lstat(abs)
		if err != nil || !fi.Mode().IsRegular() {
			continue
		}
		return abs, true
	}
	return "", false
}

// PathUnderAny reports whether absPath is inside one of the allowed tenant roots.
func PathUnderAny(roots []string, absPath string) bool {
	path := filepath.Clean(absPath)
	for _, root := range roots {
		rel, err := filepath.Rel(filepath.Clean(root), path)
		if err != nil {
			continue
		}
		if rel == "." {
			return true
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}
