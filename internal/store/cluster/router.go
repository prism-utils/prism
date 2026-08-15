package cluster

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/prism-utils/prism/internal/store/httperr"
	storeingest "github.com/prism-utils/prism/internal/store/ingest"
	"github.com/prism-utils/prism/internal/store/query"
	storetenant "github.com/prism-utils/prism/internal/store/tenant"
)

const (
	proxyDialTimeout        = 10 * time.Second
	proxyResponseHeaderWait = 30 * time.Second
	proxyIdleConnTimeout    = 90 * time.Second
)

// Router forwards query requests to the client that owns the tenant namespace.
type Router struct {
	clients map[string]*url.URL
	proxies map[string]*httputil.ReverseProxy
	mu      sync.Mutex
}

// NewRouter builds a coordinator router from a tenant-to-base-URL map.
func NewRouter(clients map[string]*url.URL) *Router {
	return &Router{
		clients: clients,
		proxies: make(map[string]*httputil.ReverseProxy),
	}
}

// NewServeMux registers health endpoints and the query route for cluster mode.
func NewServeMux(clients map[string]*url.URL, routePrefix string, wrapQuery func(http.Handler) http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReadyz)
	q := http.Handler(NewRouter(clients))
	if wrapQuery != nil {
		q = wrapQuery(q)
	}
	mux.Handle(query.QueryRoutePattern(routePrefix), q)
	mux.Handle(query.SQLRoutePattern(routePrefix), q)
	// PromQL read API is per-tenant like structured query, so the coordinator
	// forwards each pattern to the single owning client the same way.
	for _, pattern := range query.PromQLRoutePatterns(routePrefix) {
		mux.Handle(pattern, q)
	}
	// The Loki logs read API is likewise per-tenant and forwards unchanged.
	for _, pattern := range query.LokiRoutePatterns(routePrefix) {
		mux.Handle(pattern, q)
	}
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

// ServeHTTP implements http.Handler.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("ns")
	if !storeingest.ValidateTenant(ns) {
		http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
		return
	}
	target, ok := r.clients[ns]
	if !ok {
		http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
		return
	}
	r.proxyFor(target).ServeHTTP(w, req)
}

func (r *Router) proxyFor(target *url.URL) *httputil.ReverseProxy {
	key := target.String()
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.proxies[key]; ok {
		return p
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   proxyDialTimeout,
			KeepAlive: proxyDialTimeout,
		}).DialContext,
		ResponseHeaderTimeout: proxyResponseHeaderWait,
		IdleConnTimeout:       proxyIdleConnTimeout,
	}
	base := *target
	p := &httputil.ReverseProxy{
		Transport:     transport,
		FlushInterval: -1,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(&base)
			pr.Out.URL.Path = pr.In.URL.Path
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			if httperr.IsCanceled(err) {
				httperr.Write(w)
				return
			}
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
	}
	r.proxies[key] = p
	return p
}
