// Command prism runs config-driven observability pipelines: it loads a pipeline
// set, wires the built-in components, and streams inputs → parsers → buffer →
// fan-out branches until interrupted.
//
// Usage:
//
//	prism run      -config prism.yaml
//	prism validate -config prism.yaml
//	prism collect  -addr :8815 -dir ./ingest
//	prism version
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/prism-utils/prism/internal/collect"
	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/components"
	"github.com/prism-utils/prism/internal/config"
	"github.com/prism-utils/prism/internal/obs"
	"github.com/prism-utils/prism/internal/pipeline"
	"github.com/prism-utils/prism/internal/quick"
	"github.com/prism-utils/prism/internal/version"
)

func writeVersion(w io.Writer) error {
	_, err := fmt.Fprintf(w, "prism %s\n", version.Version)
	return err
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = runCmd(os.Args[2:])
	case "validate":
		err = validateCmd(os.Args[2:])
	case "collect":
		err = collectCmd(os.Args[2:])
	case "version":
		if err := writeVersion(os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "prism:", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "prism:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `prism — config-driven observability pipelines

usage:
  prism run      -config <file>       run pipelines until interrupted
  prism run      --quick logs         run a built-in preset (stdin → template→count on stdout)
                 [--store <url>]      also ship logs-summary parquet to a prism-store
                 [--tenant <ns>]      tenant namespace for --store (default "default")
                 [--token <t>]        bearer token for --store ingest
                 [--print-config]     print the effective preset config and exit
  prism validate -config <file>       load and validate a config, then exit
  prism collect  -addr <a> -dir <d>   run an Arrow Flight receiver → Parquet
                 [-token <t>]         require a bearer token on every RPC
  prism version                       print version
`)
}

func collectCmd(args []string) error {
	fs := flag.NewFlagSet("collect", flag.ContinueOnError)
	addr := fs.String("addr", ":8815", "address to bind the Flight receiver on")
	dir := fs.String("dir", "", "directory to persist received windows as Parquet")
	token := fs.String("token", "", "require this bearer token on every RPC (or ${PRISM_COLLECT_TOKEN})")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("-dir is required")
	}
	tok := *token
	if tok == "" {
		tok = os.Getenv("PRISM_COLLECT_TOKEN")
	}
	logger := obs.NewLogger(os.Stderr, 0)
	var opts []collect.Option
	if tok != "" {
		opts = append(opts, collect.WithToken(tok))
	}
	srv, err := collect.NewServer(*dir, logger, opts...)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return srv.Serve(ctx, *addr, func(bound string) {
		logger.Info("prism collect listening", "addr", bound, "dir", *dir, "version", version.Version)
	})
}

func loadConfig(args []string) (*config.Config, error) {
	fs := flag.NewFlagSet("prism", flag.ContinueOnError)
	path := fs.String("config", "", "path to the pipeline config (YAML or JSON)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if *path == "" {
		return nil, fmt.Errorf("-config is required")
	}
	return config.LoadFile(*path)
}

func validateCmd(args []string) error {
	cfg, err := loadConfig(args)
	if err != nil {
		return err
	}
	fmt.Printf("ok: %d pipeline(s) valid\n", len(cfg.Pipelines))
	return nil
}

// runOptions holds the parsed flags for the run subcommand. Exactly one of
// configPath or quickTemplate selects the pipeline source.
type runOptions struct {
	configPath    string
	quickTemplate string
	store         string
	tenant        string
	token         string
	printConfig   bool
}

func parseRunFlags(args []string) (runOptions, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var o runOptions
	fs.StringVar(&o.configPath, "config", "", "path to the pipeline config (YAML or JSON)")
	fs.StringVar(&o.quickTemplate, "quick", "", "run a built-in preset instead of a config file (e.g. \"logs\")")
	fs.StringVar(&o.store, "store", "", "prism-store base URL to also ship logs to (quick mode)")
	fs.StringVar(&o.tenant, "tenant", "", "tenant namespace for --store (default \"default\")")
	fs.StringVar(&o.token, "token", "", "bearer token for --store ingest")
	fs.BoolVar(&o.printConfig, "print-config", false, "print the effective config and exit")
	if err := fs.Parse(args); err != nil {
		return runOptions{}, err
	}
	return o, nil
}

// runConfig resolves the run subcommand's flags into a validated config,
// choosing the quick preset or a config file. Mixing the two is an error.
func runConfig(o *runOptions) (*config.Config, error) {
	switch {
	case o.quickTemplate != "" && o.configPath != "":
		return nil, fmt.Errorf("run: -config and --quick are mutually exclusive")
	case o.quickTemplate != "":
		return quick.Build(o.quickTemplate, quick.Options{Store: o.store, Tenant: o.tenant, Token: o.token})
	case o.configPath != "":
		return config.LoadFile(o.configPath)
	default:
		return nil, fmt.Errorf("run: one of -config or --quick is required")
	}
}

func runCmd(args []string) error {
	o, err := parseRunFlags(args)
	if err != nil {
		return err
	}
	cfg, err := runConfig(&o)
	if err != nil {
		return err
	}
	if o.printConfig {
		return printEffectiveConfig(os.Stdout, cfg)
	}

	reg, err := components.Default()
	if err != nil {
		return err
	}
	logger := obs.NewLogger(os.Stderr, 0)
	set, err := pipeline.Build(cfg, reg, component.Settings{Logger: logger})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("prism starting", "pipelines", len(cfg.Pipelines), "version", version.Version)
	if o.quickTemplate == "logs" && o.store != "" {
		logger.Info("logs available on prism-store",
			"ingest", quick.IngestURL(o.store, o.tenant),
			"query_endpoint", "POST "+quick.SQLEndpoint(o.store, o.tenant),
			"example_query", quick.ExampleLogsQuery,
		)
	}
	if err := set.Run(ctx, obs.NewHost(logger)); err != nil {
		return err
	}
	logger.Info("prism stopped")
	return nil
}

// printEffectiveConfig writes the resolved config as indented JSON (valid YAML)
// so a user can inspect a quick preset or graduate it into a file.
func printEffectiveConfig(w io.Writer, cfg *config.Config) error {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("run: encode config: %w", err)
	}
	_, err = fmt.Fprintln(w, string(b))
	return err
}
