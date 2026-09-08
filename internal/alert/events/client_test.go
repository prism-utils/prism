package events

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prism-utils/prism/internal/alert/notify"
)

func TestClientPostsParquetToAlertEventsIngest(t *testing.T) {
	var gotPath string
	var gotAuth string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = b
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("reader-jwt\n"), 0o600))

	c, err := NewClient(srv.URL, "/prism-proxy", "user-abc-apps", tokenFile, srv.Client(), nil)
	require.NoError(t, err)
	c.Persist(t.Context(), []notify.Alert{{
		Labels:   map[string]string{"alertname": "A"},
		StartsAt: time.Unix(10, 0).UTC(),
	}})
	assert.Equal(t, "/prism-proxy/user-abc-apps/ingest/alert-events", gotPath)
	assert.Equal(t, "Bearer reader-jwt", gotAuth)
	assert.True(t, len(gotBody) > 4 && string(gotBody[:4]) == "PAR1")
}

func TestClientEmptyAlertsIsNoop(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	c, err := NewClient(srv.URL, "", "user-abc-apps", "", srv.Client(), nil)
	require.NoError(t, err)
	c.Persist(t.Context(), nil)
	assert.Equal(t, int32(0), hits.Load())
}

func TestClientFailOpenOnStoreError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	c, err := NewClient(srv.URL, "", "user-abc-apps", "", srv.Client(), nil)
	require.NoError(t, err)
	// Must not panic; persist is fail-open.
	c.Persist(t.Context(), []notify.Alert{{
		Labels:   map[string]string{"alertname": "A"},
		StartsAt: time.Unix(1, 0).UTC(),
	}})
}

func TestClientUsesRoutePrefixAndTenant(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	c, err := NewClient(srv.URL, "", "ns1", "", srv.Client(), nil)
	require.NoError(t, err)
	c.Persist(t.Context(), []notify.Alert{{Labels: map[string]string{"alertname": "A"}, StartsAt: time.Unix(1, 0).UTC()}})
	assert.Equal(t, "/ns1/ingest/alert-events", got)
}
