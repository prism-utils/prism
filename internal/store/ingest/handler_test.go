package ingest_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/ingest"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

const testTenant = "user-6f3a9c2b-apps"

func testConfig(token string, mode ingest.AuthMode) ingest.Config {
	return ingest.Config{
		AllowedArtifacts: []string{"metrics-raw"},
		MaxBodyBytes:     1 << 20,
		IngestToken:      token,
		AuthMode:         mode,
	}
}

func doIngestReq(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func newIngestReq(t *testing.T, url string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	return req
}

func closeResp(t *testing.T, resp *http.Response) {
	t.Helper()
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
}

func testServer(t *testing.T, cfg *ingest.Config) (http.Handler, *engine.Engine) {
	t.Helper()
	eng := engine.New(engine.Config{DataDir: t.TempDir(), HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.Handle(ingest.IngestRoutePattern(cfg.RoutePrefix), ingest.Handler(cfg, eng, logger))
	return mux, eng
}

func testHandler(t *testing.T, cfg ingest.Config) (http.Handler, *engine.Engine) { //nolint:gocritic // test helper mirrors production config shape
	t.Helper()
	return testServer(t, &cfg)
}

func ingestURL(base, prefix, ns, artifact string) string {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return base + "/" + ns + "/ingest/" + artifact
	}
	return base + prefix + "/" + ns + "/ingest/" + artifact
}

func validWindowBody(t *testing.T) *os.File {
	t.Helper()
	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestIngestHappyPath(t *testing.T) {
	h, eng := testHandler(t, testConfig("", ingest.AuthNone))
	srv := httptest.NewServer(h)
	defer srv.Close()

	f := validWindowBody(t)
	resp := doIngestReq(t, newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), f))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
}

func TestIngestEmptyBody204(t *testing.T) {
	h, eng := testHandler(t, testConfig("", ingest.AuthNone))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp := doIngestReq(t, newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), strings.NewReader("")))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 0 {
		t.Fatalf("hot rows = %d, want 0", c)
	}
}

// errReader fails on the first Read (simulates a client hanging up mid-body).
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestIngestClientAbortReturns499(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthNone))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/"+testTenant+"/ingest/metrics-raw", errReader{err: io.ErrUnexpectedEOF})
	req.Header.Set("Content-Type", "application/vnd.apache.parquet")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 499 {
		t.Fatalf("want 499, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestIngestUnknownTenant404(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthNone))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp := doIngestReq(t, newIngestReq(t, ingestURL(srv.URL, "", "INVALID", "metrics-raw"), validWindowBody(t)))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}

func TestIngestUnknownArtifact404(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthNone))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp := doIngestReq(t, newIngestReq(t, ingestURL(srv.URL, "", testTenant, "logs-raw"), validWindowBody(t)))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}

func TestIngestBodyTooLarge413(t *testing.T) {
	cfg := testConfig("", ingest.AuthNone)
	cfg.MaxBodyBytes = 8
	h, _ := testHandler(t, cfg)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp := doIngestReq(t, newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), strings.NewReader("0123456789")))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d", resp.StatusCode)
	}
}

func TestIngestRoutePrefix(t *testing.T) {
	cfg := testConfig("", ingest.AuthNone)
	cfg.RoutePrefix = "/prism-proxy"
	h, eng := testHandler(t, cfg)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp := doIngestReq(t, newIngestReq(t, ingestURL(srv.URL, "/prism-proxy", testTenant, "metrics-raw"), validWindowBody(t)))
	closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("prefixed want 204, got %d", resp.StatusCode)
	}

	resp2 := doIngestReq(t, newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t)))
	closeResp(t, resp2)
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("unprefixed want 404, got %d", resp2.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
}

func TestIngestBearerMissing401(t *testing.T) {
	h, _ := testHandler(t, testConfig("s3cret", ingest.AuthBearer))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp := doIngestReq(t, newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), strings.NewReader("x")))
	closeResp(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func TestIngestBearerWrong401(t *testing.T) {
	h, _ := testHandler(t, testConfig("s3cret", ingest.AuthBearer))
	srv := httptest.NewServer(h)
	defer srv.Close()

	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), strings.NewReader("x"))
	req.Header.Set("Authorization", "Bearer wrong")
	resp := doIngestReq(t, req)
	closeResp(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func TestIngestBearerCorrect204(t *testing.T) {
	h, eng := testHandler(t, testConfig("s3cret", ingest.AuthBearer))
	srv := httptest.NewServer(h)
	defer srv.Close()

	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	req.Header.Set("Authorization", "Bearer s3cret")
	resp := doIngestReq(t, req)
	closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
}

func TestIngestTrustedHeaderMismatch403(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthTrustedHeader))
	srv := httptest.NewServer(h)
	defer srv.Close()

	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	req.Header.Set("X-Tenant", "other-tenant")
	resp := doIngestReq(t, req)
	closeResp(t, resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %d", resp.StatusCode)
	}
}

func TestIngestTrustedHeaderMatch204(t *testing.T) {
	h, eng := testHandler(t, testConfig("", ingest.AuthTrustedHeader))
	srv := httptest.NewServer(h)
	defer srv.Close()

	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	req.Header.Set("X-Tenant", testTenant)
	resp := doIngestReq(t, req)
	closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
}

func TestIngestTrustedHeaderMissing401(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthTrustedHeader))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp := doIngestReq(t, newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t)))
	closeResp(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func TestIngestMTLSNoCert401(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthMTLS))
	ca, caKey := testCA(t)
	srvCert := testServerTLSCert(t, ca, caKey)
	srv := newMTLSServer(t, h, ca, &srvCert)
	defer srv.Close()

	client := mtlsClient(t, ca, nil)
	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	closeResp(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 without client cert, got %d", resp.StatusCode)
	}
}

func TestIngestMTLSWrongCN403(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthMTLS))
	ca, caKey := testCA(t)
	srvCert := testServerTLSCert(t, ca, caKey)
	srv := newMTLSServer(t, h, ca, &srvCert)
	defer srv.Close()

	wrongCert := testClientCert(t, ca, caKey, "wrong-tenant")
	client := mtlsClient(t, ca, &wrongCert)
	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	closeResp(t, resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403 for CN mismatch, got %d", resp.StatusCode)
	}
}

func TestIngestMTLSCNMatch204(t *testing.T) {
	h, eng := testHandler(t, testConfig("", ingest.AuthMTLS))
	ca, caKey := testCA(t)
	srvCert := testServerTLSCert(t, ca, caKey)
	srv := newMTLSServer(t, h, ca, &srvCert)
	defer srv.Close()

	clientCert := testClientCert(t, ca, caKey, testTenant)
	client := mtlsClient(t, ca, &clientCert)
	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
}

func testCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return ca, key
}

func testServerTLSCert(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

func testClientCert(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, cn string) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

func newMTLSServer(t *testing.T, h http.Handler, ca *x509.Certificate, srvCert *tls.Certificate) *httptest.Server {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{*srvCert},
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}
	srv.StartTLS()
	return srv
}

func mtlsClient(t *testing.T, ca *x509.Certificate, cert *tls.Certificate) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	cfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}
	if cert != nil {
		cfg.Certificates = []tls.Certificate{*cert}
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: cfg},
	}
}
