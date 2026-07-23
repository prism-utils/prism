package authz

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/elk-utilities/prism/internal/store/auth"
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
)

type ctxKeyPrincipal struct{}

// Middleware wraps HTTP handlers with JWT authentication and RBAC authorization.
type Middleware struct {
	verifier   auth.Verifier
	authorizer *Authorizer
	logger     *slog.Logger
}

// NewMiddleware builds RBAC HTTP middleware.
func NewMiddleware(verifier auth.Verifier, authorizer *Authorizer, logger *slog.Logger) *Middleware {
	return &Middleware{verifier: verifier, authorizer: authorizer, logger: logger}
}

// WrapQuery protects GET /{ns}/query routes.
func (m *Middleware) WrapQuery(next http.Handler) http.Handler {
	return m.wrapTenantAction(ActionQuery, next)
}

// WrapSQL protects POST /{ns}/sql routes.
func (m *Middleware) WrapSQL(next http.Handler) http.Handler {
	return m.wrapTenantAction(ActionQuery, next)
}

// WrapIngest protects POST /{ns}/ingest/{artifact} routes.
func (m *Middleware) WrapIngest(next http.Handler) http.Handler {
	return m.wrapTenantAction(ActionIngest, next)
}

// WrapEnsure protects POST /admin/tenants/{ns}/ensure routes.
func (m *Middleware) WrapEnsure(next http.Handler) http.Handler {
	return m.wrapTenantAction(ActionEnsure, next)
}

// WrapStats protects GET /stats with tenant scoping via context.
func (m *Middleware) WrapStats(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := m.authenticate(w, r)
		if !ok {
			return
		}
		ns := strings.TrimSpace(r.URL.Query().Get("ns"))
		if ns != "" {
			if !storetenant.TenantAllowed(ns) {
				http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
				return
			}
			switch m.authorizer.Authorize(principal, ActionStats, ns) {
			case DecisionAllow:
				ctx := context.WithValue(r.Context(), ctxKeyStatsScope{}, TenantScope{Tenants: []string{ns}})
				next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, ctxKeyPrincipal{}, principal)))
			case DecisionDenyForbidden:
				m.logDeny(principal, ActionStats, ns, "forbidden")
				http.Error(w, "forbidden", http.StatusForbidden)
			default:
				m.logDeny(principal, ActionStats, ns, "not_found")
				http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			}
			return
		}
		scope := m.authorizer.AuthorizedTenants(principal, ActionStats)
		if !scope.All && len(scope.Tenants) == 0 {
			m.logDeny(principal, ActionStats, "", "forbidden")
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		ctx := context.WithValue(r.Context(), ctxKeyStatsScope{}, scope)
		ctx = context.WithValue(ctx, ctxKeyPrincipal{}, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (m *Middleware) wrapTenantAction(action Action, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := m.authenticate(w, r)
		if !ok {
			return
		}
		ns := strings.TrimSpace(r.PathValue("ns"))
		if !storetenant.TenantAllowed(ns) {
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}
		switch m.authorizer.Authorize(principal, action, ns) {
		case DecisionAllow:
			ctx := context.WithValue(r.Context(), ctxKeyPrincipal{}, principal)
			next.ServeHTTP(w, r.WithContext(ctx))
		case DecisionDenyForbidden:
			m.logDeny(principal, action, ns, "forbidden")
			http.Error(w, "forbidden", http.StatusForbidden)
		default:
			m.logDeny(principal, action, ns, "not_found")
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
		}
	})
}

func (m *Middleware) authenticate(w http.ResponseWriter, r *http.Request) (string, bool) {
	_ = r.Header.Get("X-User")
	_ = r.Header.Get("X-Tenant")
	token, err := bearerToken(r.Header.Get("Authorization"))
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return "", false
	}
	principal, err := m.verifier.Verify(r.Context(), token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return "", false
	}
	return principal, true
}

func (m *Middleware) logDeny(principal string, action Action, tenant, reason string) {
	if m.logger == nil {
		return
	}
	m.logger.Warn("authz denied",
		"subject", principal,
		"action", string(action),
		"tenant", tenant,
		"reason", reason,
	)
}

func bearerToken(header string) (string, error) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", auth.ErrMissingToken
	}
	tok := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if tok == "" {
		return "", auth.ErrMissingToken
	}
	return tok, nil
}

type ctxKeyStatsScope struct{}

// StatsScopeFromContext returns the stats tenant scope set by RBAC middleware.
func StatsScopeFromContext(ctx context.Context) (TenantScope, bool) {
	v, ok := ctx.Value(ctxKeyStatsScope{}).(TenantScope)
	return v, ok
}

// PrincipalFromContext returns the authenticated subject when RBAC middleware ran.
func PrincipalFromContext(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(ctxKeyPrincipal{}).(string)
	return v, ok
}
