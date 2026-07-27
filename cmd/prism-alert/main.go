// Command prism-alert is a per-tenant PromQL ruler with Alertmanager-compatible
// notification. It loads Prometheus alerting-rule YAML, evaluates each rule
// against prism-store's PromQL read API at the current time, runs the
// for/keep_firing_for/resolve state machine with $value/$labels templating,
// groups alerts Alertmanager-style, and POSTs the identical Alertmanager v4
// webhook to the prism notifier.
//
// Usage:
//
//	prism-alert          start the ruler (default)
//	prism-alert serve    start the ruler
//	prism-alert version  print version
//
// Configuration is via environment variables (secrets via ${ENV}); a few
// non-secret operational fields also accept flags that override the
// environment. See docs/ALERTING.md and docs/CONFIG.md §15.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/elk-utilities/prism/internal/alert/config"
	"github.com/elk-utilities/prism/internal/alert/notify"
	"github.com/elk-utilities/prism/internal/alert/ruler"
	"github.com/elk-utilities/prism/internal/version"
)

const (
	readHeaderTimeout  = 15 * time.Second
	shutdownTimeout    = 10 * time.Second
	dispatchResolution = time.Second
)

func versionLine() string {
	return fmt.Sprintf("prism-alert %s", version.Version)
}

// parseFlags overlays non-secret operational flags onto a config loaded from the
// environment. Secrets (WEBHOOK_SECRET, token file contents) are env/file only.
func parseFlags(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("prism-alert", flag.ContinueOnError)
	fs.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "health/probe listen address")
	fs.StringVar(&cfg.RulesDir, "rules-dir", cfg.RulesDir, "directory of Prometheus rule YAML")
	fs.StringVar(&cfg.StoreBaseURL, "store-base-url", cfg.StoreBaseURL, "prism-store query base URL")
	fs.StringVar(&cfg.TenantNS, "tenant", cfg.TenantNS, "tenant namespace")
	fs.StringVar(&cfg.NotifierWebhookURL, "notifier-url", cfg.NotifierWebhookURL, "notifier /webhook URL")
	fs.DurationVar(&cfg.EvaluationInterval, "evaluation-interval", cfg.EvaluationInterval, "rule evaluation cadence")
	return fs.Parse(args)
}

// healthMux serves liveness and readiness probes. The ruler has no external
// dependency to verify at readiness beyond having started, so readyz reports
// ready once the process is serving.
func healthMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}

func dispatcherOptions(cfg *config.Config) notify.Options {
	return notify.Options{
		Receiver:       cfg.Receiver,
		ExternalURL:    cfg.ExternalURL,
		GroupBy:        cfg.GroupBy,
		GroupWait:      cfg.GroupWait,
		GroupInterval:  cfg.GroupInterval,
		RepeatInterval: cfg.RepeatInterval,
		ResolveTimeout: cfg.ResolveTimeout,
	}
}

func run(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	client, err := ruler.NewPromQLClient(cfg.StoreBaseURL, cfg.RoutePrefix, cfg.TenantNS, cfg.StoreTokenFile, nil)
	if err != nil {
		return fmt.Errorf("build promql client: %w", err)
	}

	webhook := notify.NewWebhookClient(notify.WebhookConfig{URL: cfg.NotifierWebhookURL, Secret: cfg.WebhookSecret}, logger)
	dispatcher := notify.NewDispatcher(dispatcherOptions(cfg), webhook, logger)

	r, err := ruler.New(ruler.Config{
		RulesDir:           cfg.RulesDir,
		EvaluationInterval: cfg.EvaluationInterval,
		ExternalURL:        cfg.ExternalURL,
	}, client.Query, dispatcher.Ingest, logger, time.Now)
	if err != nil {
		return fmt.Errorf("build ruler: %w", err)
	}

	srv := &http.Server{Addr: cfg.ListenAddr, Handler: healthMux(), ReadHeaderTimeout: readHeaderTimeout}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("prism-alert starting",
			"tenant", cfg.TenantNS,
			"store_base_url", cfg.StoreBaseURL,
			"notifier_webhook_url", cfg.NotifierWebhookURL,
			"rules_dir", cfg.RulesDir,
			"rule_files", len(r.Files()),
			"evaluation_interval", cfg.EvaluationInterval.String(),
			"listen_addr", cfg.ListenAddr,
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("health listen: %w", err)
		}
	}()

	go dispatcher.Run(ctx, time.Now, dispatchResolution)
	go func() {
		if err := r.Run(ctx); err != nil {
			logger.Error("ruler stopped", "err", err)
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	logger.Info("prism-alert stopped")
	return nil
}

func main() {
	args := os.Args[1:]
	if len(args) >= 1 && args[0] == "version" {
		fmt.Println(versionLine())
		return
	}
	if len(args) >= 1 && args[0] == "serve" {
		args = args[1:]
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}
	if err := parseFlags(&cfg, args); err != nil {
		os.Exit(2)
	}
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, &cfg, logger); err != nil {
		logger.Error("prism-alert failed", "err", err)
		os.Exit(1)
	}
}
