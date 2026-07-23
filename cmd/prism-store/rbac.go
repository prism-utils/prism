package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/elk-utilities/prism/internal/store/admin"
	"github.com/elk-utilities/prism/internal/store/auth"
	"github.com/elk-utilities/prism/internal/store/authz"
	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
)

type rbacConfig struct {
	policyFile    string
	issuer        string
	jwksURL       string
	jwksFile      string
	audience      []string
	reloadSeconds int
}

func loadRBACConfig() *rbacConfig {
	policyFile := strings.TrimSpace(os.Getenv("AUTHZ_POLICY_FILE"))
	if policyFile == "" {
		return nil
	}
	audRaw := os.Getenv("OIDC_AUDIENCE")
	var audience []string
	for _, a := range strings.Split(audRaw, ",") {
		if a = strings.TrimSpace(a); a != "" {
			audience = append(audience, a)
		}
	}
	reload := envInt("AUTHZ_RELOAD_SECONDS", 15)
	return &rbacConfig{
		policyFile:    policyFile,
		issuer:        strings.TrimSpace(os.Getenv("OIDC_ISSUER")),
		jwksURL:       strings.TrimSpace(os.Getenv("OIDC_JWKS_URL")),
		jwksFile:      strings.TrimSpace(os.Getenv("OIDC_JWKS_FILE")),
		audience:      audience,
		reloadSeconds: reload,
	}
}

type rbacStack struct {
	middleware *authz.Middleware
	authorizer *authz.Authorizer
	verifier   auth.Verifier
}

func (s *rbacStack) close() {
	if s == nil || s.authorizer == nil {
		return
	}
	s.authorizer.Close()
}

func buildRBACStack(ctx context.Context, cfg *rbacConfig, logger *slog.Logger) (*rbacStack, error) {
	if cfg == nil {
		return nil, nil
	}
	jwtCfg := auth.JWTConfig{
		Issuer:   cfg.issuer,
		JWKSURL:  cfg.jwksURL,
		JWKSFile: cfg.jwksFile,
		Audience: cfg.audience,
	}
	if err := jwtCfg.Validate(); err != nil {
		return nil, fmt.Errorf("rbac oidc: %w", err)
	}
	verifier, err := auth.NewJWTVerifier(ctx, jwtCfg)
	if err != nil {
		return nil, fmt.Errorf("rbac verifier: %w", err)
	}
	authorizer, err := authz.NewAuthorizer(ctx, authz.Config{
		PolicyFile:    cfg.policyFile,
		ReloadSeconds: cfg.reloadSeconds,
		Logger:        logger,
	})
	if err != nil {
		return nil, fmt.Errorf("rbac authorizer: %w", err)
	}
	logger.Info("prism-store rbac enabled",
		"issuer", cfg.issuer,
		"bindings", authorizer.BindingCount(),
		"policy_file", cfg.policyFile,
	)
	return &rbacStack{
		middleware: authz.NewMiddleware(verifier, authorizer, logger),
		authorizer: authorizer,
		verifier:   verifier,
	}, nil
}

func (s *rbacStack) wrapQuery(h http.Handler) http.Handler {
	if s == nil {
		return h
	}
	return s.middleware.WrapQuery(h)
}

func (s *rbacStack) wrapIngest(h http.Handler) http.Handler {
	if s == nil {
		return h
	}
	return s.middleware.WrapIngest(h)
}

func (s *rbacStack) wrapEnsure(h http.Handler) http.Handler {
	if s == nil {
		return h
	}
	return s.middleware.WrapEnsure(h)
}

func (s *rbacStack) wrapStats(h http.Handler) http.Handler {
	if s == nil {
		return h
	}
	return s.middleware.WrapStats(h)
}

func ingestAuthMode(cfg *serverConfig, rbac *rbacStack) (storeingest.AuthMode, error) {
	if rbac != nil {
		return storeingest.AuthNone, nil
	}
	return storeingest.ParseAuthMode(cfg.authMode)
}

func protectAdminRoute(rbac *rbacStack, token string, wrap func(http.Handler) http.Handler, h http.Handler) http.Handler {
	if rbac != nil {
		return wrap(h)
	}
	return admin.WithBearerAuth(token, h)
}
