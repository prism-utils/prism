package ruler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromQLClientParsesVector(t *testing.T) {
	var gotPath, gotQuery, gotTime, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.Query().Get("query")
		gotTime = r.URL.Query().Get("time")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"__name__":"up","instance":"n1"},"value":[1700000000,"1"]},
			{"metric":{"__name__":"up","instance":"n2"},"value":[1700000000,"0"]}
		]}}`))
	}))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("reader-jwt\n"), 0o600))

	c, err := NewPromQLClient(srv.URL, "/prism", "team-a", tokenFile, nil)
	require.NoError(t, err)

	vec, err := c.Query(context.Background(), `up`, time.Unix(1700000000, 0))
	require.NoError(t, err)
	require.Len(t, vec, 2)
	assert.Equal(t, "up", vec[0].Metric.Get("__name__"))
	assert.Equal(t, 1.0, vec[0].F)
	assert.Equal(t, 0.0, vec[1].F)

	assert.Equal(t, "/prism/team-a/api/v1/query", gotPath)
	assert.Equal(t, "up", gotQuery)
	assert.Equal(t, "1700000000", gotTime)
	assert.Equal(t, "Bearer reader-jwt", gotAuth, "token read fresh from file, whitespace trimmed")
}

func TestPromQLClientParsesScalar(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"scalar","result":[1700000000,"42"]}}`))
	}))
	defer srv.Close()

	c, err := NewPromQLClient(srv.URL, "", "t", "", nil)
	require.NoError(t, err)
	vec, err := c.Query(context.Background(), `40+2`, time.Unix(1700000000, 0))
	require.NoError(t, err)
	require.Len(t, vec, 1)
	assert.Equal(t, 42.0, vec[0].F)
	assert.Equal(t, 0, vec[0].Metric.Len(), "scalar carries an empty label set")
}

func TestPromQLClientNoTokenFileNoAuthHeader(t *testing.T) {
	var hadAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hadAuth = r.Header["Authorization"]
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer srv.Close()

	c, err := NewPromQLClient(srv.URL, "", "t", "", nil)
	require.NoError(t, err)
	_, err = c.Query(context.Background(), `up`, time.Unix(1, 0))
	require.NoError(t, err)
	assert.False(t, hadAuth, "no token file → no Authorization header")
}

func TestPromQLClientErrorsOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c, err := NewPromQLClient(srv.URL, "", "t", "", nil)
	require.NoError(t, err)
	_, err = c.Query(context.Background(), `up`, time.Unix(1, 0))
	require.Error(t, err)
}

func TestPromQLClientErrorsOnAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"parse error"}`))
	}))
	defer srv.Close()

	c, err := NewPromQLClient(srv.URL, "", "t", "", nil)
	require.NoError(t, err)
	_, err = c.Query(context.Background(), `up{`, time.Unix(1, 0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse error")
}

func TestPromQLClientErrorsWhenTokenFileMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer srv.Close()

	c, err := NewPromQLClient(srv.URL, "", "t", "/nonexistent/token", nil)
	require.NoError(t, err)
	_, err = c.Query(context.Background(), `up`, time.Unix(1, 0))
	require.Error(t, err, "a configured but unreadable token must fail the eval, not fall back to no auth")
}
