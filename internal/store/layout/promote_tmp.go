package layout

import (
	"encoding/hex"
	"path/filepath"
	"strings"
)

// PromoteTempSuffix marks an unfinished cross-root copy. The suffix is not a
// live segment extension, so listings that open parquet/duckdb skip it.
const PromoteTempSuffix = ".promote.tmp"

// IsPromoteTemp reports whether a directory entry is an unfinished promote copy.
func IsPromoteTemp(name string) bool {
	return len(name) > len(PromoteTempSuffix) && strings.HasSuffix(name, PromoteTempSuffix)
}

// PromoteTempPath names a unique temp file in destDir for copying onto finalName.
func PromoteTempPath(destDir, finalName string, id []byte) string {
	return filepath.Join(destDir, finalName+"."+hex.EncodeToString(id)+PromoteTempSuffix)
}
