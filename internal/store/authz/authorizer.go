package authz

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	storetenant "github.com/prism-utils/prism/internal/store/tenant"
	"gopkg.in/yaml.v3"
)

const wildcardTenant = "*"

// Binding ties a subject to a role over one or more tenants.
type Binding struct {
	Subject string   `yaml:"subject"`
	Role    string   `yaml:"role"`
	Tenants []string `yaml:"tenants"`
}

// PolicyDocument is the on-disk RBAC policy shape.
type PolicyDocument struct {
	Bindings []Binding `yaml:"bindings"`
}

// compiledBinding is a validated binding entry.
type compiledBinding struct {
	subject string
	role    Role
	tenants map[string]struct{}
	all     bool
}

type compiledPolicy struct {
	bySubject map[string][]compiledBinding
}

// Decision is the outcome of an authorization check.
type Decision int

// Authorization outcomes returned by Authorizer.Authorize.
const (
	DecisionAllow Decision = iota
	DecisionDenyNotFound
	DecisionDenyForbidden
)

// Authorizer evaluates deny-by-default RBAC policy.
type Authorizer struct {
	mu       sync.RWMutex
	policy   compiledPolicy
	filePath string
	modTime  time.Time
	reload   time.Duration
	logger   denyLogger
	stop     context.CancelFunc
}

type denyLogger interface {
	Warn(msg string, args ...any)
	Info(msg string, args ...any)
}

// Config configures policy loading and reload polling.
type Config struct {
	PolicyFile    string
	ReloadSeconds int
	Logger        denyLogger
}

// NewAuthorizer loads the initial policy and starts reload polling when configured.
func NewAuthorizer(ctx context.Context, cfg Config) (*Authorizer, error) {
	if strings.TrimSpace(cfg.PolicyFile) == "" {
		return nil, fmt.Errorf("authz: AUTHZ_POLICY_FILE is required")
	}
	reload := 15 * time.Second
	if cfg.ReloadSeconds > 0 {
		reload = time.Duration(cfg.ReloadSeconds) * time.Second
	}
	a := &Authorizer{
		filePath: cfg.PolicyFile,
		reload:   reload,
		logger:   cfg.Logger,
	}
	if err := a.loadInitial(); err != nil {
		return nil, err
	}
	if a.reload > 0 {
		reloadCtx, cancel := context.WithCancel(ctx)
		a.stop = cancel
		go a.pollReload(reloadCtx)
	}
	return a, nil
}

// Close stops background policy reload polling.
func (a *Authorizer) Close() {
	if a == nil || a.stop == nil {
		return
	}
	a.stop()
	a.stop = nil
}

func (a *Authorizer) loadInitial() error {
	pol, mod, err := parsePolicyFile(a.filePath)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.policy = pol
	a.modTime = mod
	a.mu.Unlock()
	return nil
}

// BindingCount returns the number of bindings in the active policy.
func (a *Authorizer) BindingCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	n := 0
	for _, binds := range a.policy.bySubject {
		n += len(binds)
	}
	return n
}

// Authorize checks whether principal may perform action on tenant.
func (a *Authorizer) Authorize(principal string, action Action, tenant string) Decision {
	a.mu.RLock()
	pol := a.policy
	a.mu.RUnlock()

	bindings := pol.bySubject[principal]
	if len(bindings) == 0 {
		return DecisionDenyNotFound
	}

	tenantBound := false
	for _, b := range bindings {
		if !bindingCoversTenant(b, tenant) {
			continue
		}
		tenantBound = true
		if roleAllows(b.role, action) {
			return DecisionAllow
		}
	}
	if tenantBound {
		return DecisionDenyForbidden
	}
	return DecisionDenyNotFound
}

// TenantScope describes tenants a principal may use for an action.
type TenantScope struct {
	All     bool
	Tenants []string
}

// AuthorizedTenants lists tenants where principal has permission for action.
func (a *Authorizer) AuthorizedTenants(principal string, action Action) TenantScope {
	a.mu.RLock()
	pol := a.policy
	a.mu.RUnlock()

	bindings := pol.bySubject[principal]
	seen := make(map[string]struct{})
	var out TenantScope
	for _, b := range bindings {
		if !roleAllows(b.role, action) {
			continue
		}
		if b.all {
			out.All = true
			return out
		}
		for t := range b.tenants {
			if _, ok := seen[t]; ok {
				continue
			}
			seen[t] = struct{}{}
			out.Tenants = append(out.Tenants, t)
		}
	}
	return out
}

func bindingCoversTenant(b compiledBinding, tenant string) bool {
	if b.all {
		return true
	}
	_, ok := b.tenants[tenant]
	return ok
}

func parsePolicyFile(path string) (compiledPolicy, time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return compiledPolicy{}, time.Time{}, fmt.Errorf("authz: stat policy file: %w", err)
	}
	raw, err := os.ReadFile(path) //nolint:gosec // G703: path comes from operator config
	if err != nil {
		return compiledPolicy{}, time.Time{}, fmt.Errorf("authz: read policy file: %w", err)
	}
	pol, err := compilePolicy(raw)
	if err != nil {
		return compiledPolicy{}, time.Time{}, err
	}
	return pol, info.ModTime(), nil
}

func compilePolicy(raw []byte) (compiledPolicy, error) {
	var doc PolicyDocument
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return compiledPolicy{}, fmt.Errorf("authz: parse policy yaml: %w", err)
	}
	if len(doc.Bindings) == 0 {
		return compiledPolicy{}, fmt.Errorf("authz: policy bindings must not be empty")
	}
	out := compiledPolicy{bySubject: make(map[string][]compiledBinding)}
	type bindKey struct {
		subject string
		tenant  string
	}
	seenRole := make(map[bindKey]Role)
	for i, b := range doc.Bindings {
		path := fmt.Sprintf("bindings[%d]", i)
		subject := strings.TrimSpace(b.Subject)
		if subject == "" {
			return compiledPolicy{}, fmt.Errorf("authz: %s.subject must not be empty", path)
		}
		role, ok := parseRole(strings.TrimSpace(b.Role))
		if !ok {
			return compiledPolicy{}, fmt.Errorf("authz: %s.role %q is unknown", path, b.Role)
		}
		if len(b.Tenants) == 0 {
			return compiledPolicy{}, fmt.Errorf("authz: %s.tenants must not be empty", path)
		}
		compiled := compiledBinding{
			subject: subject,
			role:    role,
			tenants: make(map[string]struct{}),
		}
		for _, t := range b.Tenants {
			t = strings.TrimSpace(t)
			if t == "" {
				return compiledPolicy{}, fmt.Errorf("authz: %s.tenants contains an empty entry", path)
			}
			if t == wildcardTenant {
				compiled.all = true
				compiled.tenants = nil
				break
			}
			if !storetenant.TenantAllowed(t) {
				return compiledPolicy{}, fmt.Errorf("authz: %s.tenants %q is invalid", path, t)
			}
			key := bindKey{subject: subject, tenant: t}
			if prev, dup := seenRole[key]; dup && prev != role {
				return compiledPolicy{}, fmt.Errorf("authz: %s contradicts an earlier binding for subject %q tenant %q", path, subject, t)
			}
			seenRole[key] = role
			compiled.tenants[t] = struct{}{}
		}
		out.bySubject[subject] = append(out.bySubject[subject], compiled)
	}
	return out, nil
}

func (a *Authorizer) pollReload(ctx context.Context) {
	ticker := time.NewTicker(a.reload)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.tryReload()
		}
	}
}

func (a *Authorizer) tryReload() {
	info, err := os.Stat(a.filePath)
	if err != nil {
		if a.logger != nil {
			a.logger.Warn("authz policy reload stat failed", "err", err)
		}
		return
	}
	a.mu.RLock()
	prev := a.modTime
	a.mu.RUnlock()
	if !info.ModTime().After(prev) {
		return
	}
	pol, mod, err := parsePolicyFile(a.filePath)
	if err != nil {
		if a.logger != nil {
			a.logger.Warn("authz policy reload rejected; keeping previous policy", "err", err)
		}
		return
	}
	a.mu.Lock()
	a.policy = pol
	a.modTime = mod
	a.mu.Unlock()
	if a.logger != nil {
		a.logger.Info("authz policy reloaded", "bindings", a.BindingCount())
	}
}

// ReloadNow forces a policy reload attempt; intended for tests.
func (a *Authorizer) ReloadNow() {
	a.tryReload()
}
