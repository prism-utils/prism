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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/elk-utilities/prism/internal/collect"
	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/components"
	"github.com/elk-utilities/prism/internal/config"
	"github.com/elk-utilities/prism/internal/obs"
	"github.com/elk-utilities/prism/internal/pipeline"
)

// version is overridable at build time with -ldflags "-X main.version=…".
var version = "dev"

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
		fmt.Println("prism", version)
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
		logger.Info("prism collect listening", "addr", bound, "dir", *dir, "version", version)
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

func runCmd(args []string) error {
	cfg, err := loadConfig(args)
	if err != nil {
		return err
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
	logger.Info("prism starting", "pipelines", len(cfg.Pipelines), "version", version)
	if err := set.Run(ctx, obs.NewHost(logger)); err != nil {
		return err
	}
	logger.Info("prism stopped")
	return nil
}
