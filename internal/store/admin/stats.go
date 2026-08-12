package admin

import (
	"os"

	"github.com/prism-utils/prism/internal/store/authz"
	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/stats"
)

// ArtifactStats is per-artifact window counts for billing consumers.
type ArtifactStats struct {
	Windows         int   `json:"windows"`
	LatestUnixNanos int64 `json:"latestUnixNanos"`
}

// StatsResponse is the frozen /stats JSON billing contract.
type StatsResponse struct {
	Artifacts            map[string]ArtifactStats `json:"artifacts"`
	TotalWindows         int                      `json:"totalWindows"`
	OnDiskBytes          int64                    `json:"onDiskBytes,omitempty"`
	CompactionCpuSeconds float64                  `json:"compactionCpuSeconds,omitempty"`
}

// CollectEngineStats returns window counts and latest L0 mtime for one tenant.
func CollectEngineStats(dataDir, ns string, eng *engine.Engine) ArtifactStats {
	var st ArtifactStats
	if hot, err := eng.HotRowCount(ns); err == nil {
		st.Windows += int(hot)
	}
	l0, err := engine.ListL0(dataDir, ns)
	if err == nil {
		st.Windows += len(l0)
		for _, p := range l0 {
			if info, err := os.Stat(p); err == nil { //nolint:gosec // G703: tier segment paths stay under the tenant data root
				if ts := info.ModTime().UnixNano(); ts > st.LatestUnixNanos {
					st.LatestUnixNanos = ts
				}
			}
		}
	}
	return st
}

// CollectEngineStatsAllTenants aggregates window counts across tenant directories.
func CollectEngineStatsAllTenants(dataDir string, eng *engine.Engine) ArtifactStats {
	tenants, err := os.ReadDir(dataDir)
	if err != nil {
		return ArtifactStats{}
	}
	var agg ArtifactStats
	for _, tenant := range tenants {
		if !tenant.IsDir() {
			continue
		}
		st := CollectEngineStats(dataDir, tenant.Name(), eng)
		agg.Windows += st.Windows
		if st.LatestUnixNanos > agg.LatestUnixNanos {
			agg.LatestUnixNanos = st.LatestUnixNanos
		}
	}
	return agg
}

// BuildStatsResponse assembles the /stats payload for one or all tenants.
func BuildStatsResponse(cfg *Config, eng *engine.Engine, ns string) StatsResponse {
	resp := StatsResponse{Artifacts: make(map[string]ArtifactStats, len(cfg.AllowedArtifacts))}
	for _, artifact := range cfg.AllowedArtifacts {
		var st ArtifactStats
		if ns != "" {
			st = CollectEngineStats(cfg.DataDir, ns, eng)
		} else {
			st = CollectEngineStatsAllTenants(cfg.DataDir, eng)
		}
		_ = artifact
		resp.Artifacts[artifact] = st
		resp.TotalWindows += st.Windows
	}
	if ns != "" {
		if b, err := stats.TenantOnDiskBytes(cfg.DataDir, ns); err == nil {
			resp.OnDiskBytes = b
		}
		if s, err := stats.CompactionCPUSeconds(cfg.DataDir, ns); err == nil {
			resp.CompactionCpuSeconds = s
		}
	}
	return resp
}

// BuildStatsResponseScoped assembles /stats over an explicit tenant scope.
func BuildStatsResponseScoped(cfg *Config, eng *engine.Engine, scope authz.TenantScope) StatsResponse {
	if scope.All {
		return BuildStatsResponse(cfg, eng, "")
	}
	resp := StatsResponse{Artifacts: make(map[string]ArtifactStats, len(cfg.AllowedArtifacts))}
	for _, artifact := range cfg.AllowedArtifacts {
		var st ArtifactStats
		for _, ns := range scope.Tenants {
			t := CollectEngineStats(cfg.DataDir, ns, eng)
			st.Windows += t.Windows
			if t.LatestUnixNanos > st.LatestUnixNanos {
				st.LatestUnixNanos = t.LatestUnixNanos
			}
		}
		_ = artifact
		resp.Artifacts[artifact] = st
		resp.TotalWindows += st.Windows
	}
	return resp
}

// CollectEngineStatsForTenants aggregates window counts for the given tenant names.
func CollectEngineStatsForTenants(dataDir string, eng *engine.Engine, tenants []string) ArtifactStats {
	var agg ArtifactStats
	for _, ns := range tenants {
		st := CollectEngineStats(dataDir, ns, eng)
		agg.Windows += st.Windows
		if st.LatestUnixNanos > agg.LatestUnixNanos {
			agg.LatestUnixNanos = st.LatestUnixNanos
		}
	}
	return agg
}
