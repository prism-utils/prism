// Package quick builds ready-to-run pipeline presets for `prism run --quick`,
// so a user can pipe logs in with zero configuration:
//
//	my-app 2>&1 | prism run --quick logs
//
// A preset is just a pre-filled, validated config.Config handed to the normal
// pipeline builder — there is no second config code path. Local display is
// pure-Go (the summary is printed to stdout); an optional remote ship branch
// sends the same window to a prism-store when Options.Store is set.
package quick

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/config"
)

const (
	// DefaultTenant is the namespace used when Options.Tenant is empty.
	DefaultTenant = "default"
	// LogsSummaryArtifact is the artifact type the logs preset ships to a store.
	LogsSummaryArtifact = "logs-summary"
	// logsBufferMaxAge flushes the window quickly so quick-mode output appears
	// within a couple of seconds instead of the 30s pipeline default.
	logsBufferMaxAge = 2 * time.Second
)

// ExampleLogsQuery is the read-only SQL the logs preset advertises: the total
// count per mined template across shipped summary windows.
const ExampleLogsQuery = `SELECT template, sum(count) AS count FROM logs GROUP BY template ORDER BY count DESC`

// Options tunes a preset's optional remote fan-out. A zero Options builds a
// local-only preset whose only output is the summary printed to stdout.
type Options struct {
	// Store is a prism-store base URL (e.g. "https://store:8080"). Empty means
	// local-only: no ship branch is added.
	Store string
	// Tenant is the namespace used in the remote ingest path and query hint.
	// Empty defaults to DefaultTenant.
	Tenant string
	// Token is the bearer token sent to the store on ingest (optional).
	Token string
}

// Templates lists the known preset names.
func Templates() []string {
	return []string{"logs"}
}

// Build returns a validated preset config for the named template. The only
// template in this cut is "logs". Unknown names return an error listing the
// known templates.
func Build(template string, opts Options) (*config.Config, error) {
	switch template {
	case "logs":
		return buildLogs(opts)
	default:
		return nil, fmt.Errorf("quick: unknown template %q (known: %s)", template, strings.Join(Templates(), ", "))
	}
}

// buildLogs assembles the logs preset: stdin → logs{format:auto} → a short
// window → template+summary, printed to stdout, and optionally shipped to a
// store as logs-summary parquet.
func buildLogs(opts Options) (*config.Config, error) {
	templateProc := config.Stage{Type: "template", Options: rawOpts(map[string]any{
		"source": "message",
		"target": "template",
	})}
	summaryProc := config.Stage{Type: "summary", Options: rawOpts(map[string]any{
		"group_by":   []string{"template"},
		"aggregates": []string{"count"},
	})}

	branches := []config.Branch{{
		Name:       "summary",
		Processors: []config.Stage{templateProc, summaryProc},
		Encoder:    config.Stage{Type: "json"},
		Output:     config.Stage{Type: "stdout"},
	}}

	if opts.Store != "" {
		httpOpts := map[string]any{
			"url":          IngestURL(opts.Store, opts.Tenant),
			"content_type": "application/vnd.apache.parquet",
		}
		if opts.Token != "" {
			httpOpts["token"] = opts.Token
		}
		branches = append(branches, config.Branch{
			Name:       "ship",
			Processors: []config.Stage{templateProc, summaryProc},
			Encoder:    config.Stage{Type: "parquet", Options: rawOpts(map[string]any{"compression": "zstd"})},
			Output:     config.Stage{Type: "http", Options: rawOpts(httpOpts)},
		})
	}

	cfg := &config.Config{
		Pipelines: []config.PipelineConfig{{
			Name:     "logs",
			Input:    config.Stage{Type: "stdin", Options: rawOpts(map[string]any{"batch_size": 500})},
			Parser:   config.Stage{Type: "logs", Options: rawOpts(map[string]any{"format": "auto"})},
			Buffer:   config.Buffer{MaxAge: config.Duration(logsBufferMaxAge)},
			OnError:  "drop",
			Branches: branches,
		}},
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("quick: build logs: %w", err)
	}
	return cfg, nil
}

// TenantOr returns the tenant to use, substituting DefaultTenant for empty.
func TenantOr(tenant string) string {
	if tenant == "" {
		return DefaultTenant
	}
	return tenant
}

// IngestURL builds the store ingest URL for the logs-summary artifact.
func IngestURL(store, tenant string) string {
	return fmt.Sprintf("%s/%s/ingest/%s", strings.TrimRight(store, "/"), TenantOr(tenant), LogsSummaryArtifact)
}

// SQLEndpoint builds the store SQL endpoint for the given tenant.
func SQLEndpoint(store, tenant string) string {
	return fmt.Sprintf("%s/%s/sql", strings.TrimRight(store, "/"), TenantOr(tenant))
}

// rawOpts marshals an options map to a config Stage.Options blob. The maps built
// in this package are always JSON-serializable, so a marshal failure is a
// programming error rather than a runtime condition.
func rawOpts(m map[string]any) json.RawMessage {
	b, err := json.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("quick: marshal options: %v", err))
	}
	return b
}
