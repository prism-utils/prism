package ruler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

// PromQLClient evaluates instant PromQL queries against prism-store's
// Prometheus-compatible read API at a given time. It presents the tenant reader
// JWT, read fresh from a mounted file per request so token rotation needs no
// restart.
type PromQLClient struct {
	endpoint   string
	tokenFile  string
	httpClient *http.Client
}

// NewPromQLClient builds a client targeting
// {storeBaseURL}{routePrefix}/{tenant}/api/v1/query. A nil httpClient gets a
// 30s-timeout default.
func NewPromQLClient(storeBaseURL, routePrefix, tenant, tokenFile string, hc *http.Client) (*PromQLClient, error) {
	base, err := url.Parse(storeBaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse store base url: %w", err)
	}
	base.Path = strings.TrimRight(base.Path, "/") + path(routePrefix, tenant)
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	return &PromQLClient{endpoint: base.String(), tokenFile: tokenFile, httpClient: hc}, nil
}

func path(routePrefix, tenant string) string {
	prefix := "/" + strings.Trim(routePrefix, "/")
	if prefix == "/" {
		prefix = ""
	}
	return prefix + "/" + strings.Trim(tenant, "/") + "/api/v1/query"
}

// apiResponse is the Prometheus HTTP API envelope for an instant query.
type apiResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
}

// Query runs an instant query at time t and returns the result as a
// promql.Vector, converting a scalar result into a single empty-labelled
// sample (matching Prometheus' own EngineQueryFunc behavior). Any transport,
// status, or decode failure returns an error so the caller keeps prior alert
// state (fail-open) rather than resolving on a blip.
func (c *PromQLClient) Query(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build query request: %w", err)
	}
	params := url.Values{}
	params.Set("query", q)
	params.Set("time", strconv.FormatInt(t.Unix(), 10))
	req.URL.RawQuery = params.Encode()

	if c.tokenFile != "" {
		raw, err := os.ReadFile(c.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("read store token: %w", err)
		}
		if token := strings.TrimSpace(string(raw)); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query prism-store: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("read query response: %w", err)
	}

	var api apiResponse
	if err := json.Unmarshal(body, &api); err != nil {
		return nil, fmt.Errorf("decode query response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK || api.Status != "success" {
		if api.Error != "" {
			return nil, fmt.Errorf("prism-store query failed (%s): %s", api.ErrorType, api.Error)
		}
		return nil, fmt.Errorf("prism-store query failed with status %d", resp.StatusCode)
	}

	return decodeVector(api.Data.ResultType, api.Data.Result)
}

// jsonSample is one element of a Prometheus vector result.
type jsonSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

func decodeVector(resultType string, raw json.RawMessage) (promql.Vector, error) {
	switch resultType {
	case "vector":
		var samples []jsonSample
		if err := json.Unmarshal(raw, &samples); err != nil {
			return nil, fmt.Errorf("decode vector result: %w", err)
		}
		out := make(promql.Vector, 0, len(samples))
		for _, s := range samples {
			f, err := sampleValue(s.Value)
			if err != nil {
				return nil, err
			}
			out = append(out, promql.Sample{
				T:      timestampMillis(s.Value),
				F:      f,
				Metric: labelsFromMap(s.Metric),
			})
		}
		return out, nil
	case "scalar":
		var pair [2]any
		if err := json.Unmarshal(raw, &pair); err != nil {
			return nil, fmt.Errorf("decode scalar result: %w", err)
		}
		f, err := sampleValue(pair)
		if err != nil {
			return nil, err
		}
		return promql.Vector{promql.Sample{T: timestampMillis(pair), F: f, Metric: labels.EmptyLabels()}}, nil
	default:
		return nil, fmt.Errorf("unsupported result type %q for rule evaluation", resultType)
	}
}

func sampleValue(v [2]any) (float64, error) {
	s, ok := v[1].(string)
	if !ok {
		return 0, fmt.Errorf("sample value is not a string: %v", v[1])
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse sample value %q: %w", s, err)
	}
	return f, nil
}

func timestampMillis(v [2]any) int64 {
	if secs, ok := v[0].(float64); ok {
		return int64(secs * 1000)
	}
	return 0
}

func labelsFromMap(m map[string]string) labels.Labels {
	b := labels.NewBuilder(labels.EmptyLabels())
	for k, v := range m {
		b.Set(k, v)
	}
	return b.Labels()
}
