package ingest_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
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

func testHandler(t *testing.T, cfg ingest.Config) (http.Handler, *engine.Engine) {
	t.Helper()
	eng := engine.New(engine.Config{DataDir: t.TempDir(), HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return ingest.Handler(cfg, eng, logger), eng
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
	resp, err := http.Post(ingestURL(srv.URL, "", testTenant, "metrics-raw"), "application/octet-stream", f)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
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

	resp, err := http.Post(ingestURL(srv.URL, "", testTenant, "metrics-raw"), "application/octet-stream", strings.NewReader(""))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 0 {
		t.Fatalf("hot rows = %d, want 0", c)
	}
}

func TestIngestUnknownTenant404(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthNone))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(ingestURL(srv.URL, "", "../escape", "metrics-raw"), "application/octet-stream", validWindowBody(t))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}

func TestIngestUnknownArtifact404(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthNone))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(ingestURL(srv.URL, "", testTenant, "logs-raw"), "application/octet-stream", validWindowBody(t))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
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

	resp, err := http.Post(ingestURL(srv.URL, "", testTenant, "metrics-raw"), "application/octet-stream", strings.NewReader("0123456789"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
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

	resp, err := http.Post(ingestURL(srv.URL, "/prism-proxy", testTenant, "metrics-raw"), "application/octet-stream", validWindowBody(t))
	if err != nil {
		t.Fatalf("post prefixed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("prefixed want 204, got %d", resp.StatusCode)
	}

	resp2, err := http.Post(ingestURL(srv.URL, "", testTenant, "metrics-raw"), "application/octet-stream", validWindowBody(t))
	if err != nil {
		t.Fatalf("post unprefixed: %v", err)
	}
	resp2.Body.Close()
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

	resp, err := http.Post(ingestURL(srv.URL, "", testTenant, "metrics-raw"), "application/octet-stream", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func TestIngestBearerWrong401(t *testing.T) {
	h, _ := testHandler(t, testConfig("s3cret", ingest.AuthBearer))
	srv := httptest.NewServer(h)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, ingestURL(srv.URL, "", testTenant, "metrics-raw"), strings.NewReader("x"))
	req.Header.Set("Authorization", "Bearer wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func TestIngestBearerCorrect204(t *testing.T) {
	h, eng := testHandler(t, testConfig("s3cret", ingest.AuthBearer))
	srv := httptest.NewServer(h)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	req.Header.Set("Authorization", "Bearer s3cret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
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

	req, _ := http.NewRequest(http.MethodPost, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	req.Header.Set("X-Tenant", "other-tenant")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %d", resp.StatusCode)
	}
}

func TestIngestTrustedHeaderMatch204(t *testing.T) {
	h, eng := testHandler(t, testConfig("", ingest.AuthTrustedHeader))
	srv := httptest.NewServer(h)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	req.Header.Set("X-Tenant", testTenant)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
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

	resp, err := http.Post(ingestURL(srv.URL, "", testTenant, "metrics-raw"), "application/octet-stream", validWindowBody(t))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func TestIngestMTLSNoCert401(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthMTLS))
	srv := newMTLSServer(t, h, nil)
	defer srv.Close()

	resp, err := http.Post(ingestURL(srv.URL, "", testTenant, "metrics-raw"), "application/octet-stream", validWindowBody(t))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 without client cert, got %d", resp.StatusCode)
	}
}

func TestIngestMTLSWrongCN403(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthMTLS))
	ca, serverCert, serverKey := testCA(t)
	wrongCert, wrongKey := testClientCert(t, ca, "wrong-tenant")
	srv := newMTLSServer(t, h, &mtlsMaterial{ca: ca, serverCert: serverCert, serverKey: serverKey})
	defer srv.Close()

	client := mtlsClient(t, ca, wrongCert, wrongKey)
	req, _ := http.NewRequest(http.MethodPost, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403 for CN mismatch, got %d", resp.StatusCode)
	}
}

func TestIngestMTLSCNMatch204(t *testing.T) {
	h, eng := testHandler(t, testConfig("", ingest.AuthMTLS))
	ca, serverCert, serverKey := testCA(t)
	clientCert, clientKey := testClientCert(t, ca, testTenant)
	srv := newMTLSServer(t, h, &mtlsMaterial{ca: ca, serverCert: serverCert, serverKey: serverKey})
	defer srv.Close()

	client := mtlsClient(t, ca, clientCert, clientKey)
	req, _ := http.NewRequest(http.MethodPost, ingestURL(srv.URL, "", testTenant, "metrics-raw"), validWindowBody(t))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
}

type mtlsMaterial struct {
	ca         *x509.Certificate
	serverCert tls.Certificate
	serverKey  *rsa.PrivateKey
}

func testCA(t *testing.T) (*x509.Certificate, tls.Certificate, *rsa.PrivateKey) {
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
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return ca, pair, key
}

func testClientCert(t *testing.T, ca *x509.Certificate, cn string) (tls.Certificate, *rsa.PrivateKey) {
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
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair, key
}

func newMTLSServer(t *testing.T, h http.Handler, mat *mtlsMaterial) *httptest.Server {
	t.Helper()
	if mat == nil {
		ca, srvCert, _ := testCA(t)
		mat = &mtlsMaterial{ca: ca, serverCert: srvCert}
	}
	pool := x509.NewCertPool()
	pool.AddCert(mat.ca)
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{mat.serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}
	srv.StartTLS()
	return srv
}

func mtlsClient(t *testing.T, ca *x509.Certificate, cert tls.Certificate, key *rsa.PrivateKey) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	_ = key
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      pool,
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			},
		},
	}
}
