package http

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/udaykishore-resu/plantstream/internal/adapters/memory"
	"github.com/udaykishore-resu/plantstream/internal/bridge"
	"github.com/udaykishore-resu/plantstream/internal/collectors"
	"github.com/udaykishore-resu/plantstream/internal/domain/asset"
	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
	"github.com/udaykishore-resu/plantstream/internal/edge"
	"github.com/udaykishore-resu/plantstream/internal/ports"
)

const plantYAML = `
enterprise: acme
site: austin
sources:
  - name: plc-1
    type: sim
areas:
  - name: packaging
    lines:
      - name: line-1
        cells:
          - name: filling
            assets:
              - id: filler-01
                class: RotaryFiller
                description: 24-head filler
                source: plc-1
                tags:
                  - name: speed
                    unit: bpm
                    range: { min: 0, max: 600 }
                    sim: { signal: speed }
                  - name: state
                    datatype: UInt16
                    sim: { signal: state }
`

type fixture struct {
	srv    *httptest.Server
	broker *memory.Broker
	latest *edge.Latest
	health *collectors.Health
}

func newFixture(t *testing.T, withBridge bool) *fixture {
	t.Helper()
	p, err := asset.Parse([]byte(plantYAML))
	require.NoError(t, err)
	idx := p.Index()
	broker := memory.NewBroker()
	latest := edge.NewLatest()
	health := collectors.NewHealth()
	health.Register("plc-1", "sim")
	deps := Deps{Index: idx, Latest: latest, Health: health, Broker: broker, Version: "test", Heartbeat: 20 * time.Millisecond}
	if withBridge {
		deps.Bridge = bridge.New(bridge.Config{}, broker, memory.NewSink(0), memory.NewQueue(0), nil, nil)
	}
	s := New(deps)
	srv := httptest.NewServer(s)
	t.Cleanup(func() { srv.Close(); broker.Close() })
	return &fixture{srv: srv, broker: broker, latest: latest, health: health}
}

func (f *fixture) get(t *testing.T, path string) (int, map[string]any, http.Header) {
	t.Helper()
	resp, err := http.Get(f.srv.URL + path)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]any
	if len(body) > 0 && strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		require.NoError(t, json.Unmarshal(body, &out), string(body))
	}
	return resp.StatusCode, out, resp.Header
}

func TestHealthAndReady(t *testing.T) {
	f := newFixture(t, true)
	code, body, hdr := f.get(t, "/healthz")
	assert.Equal(t, 200, code)
	assert.Equal(t, "ok", body["status"])
	assert.NotEmpty(t, hdr.Get("X-Request-ID"))

	code, body, _ = f.get(t, "/readyz")
	assert.Equal(t, 200, code)
	comps := body["components"].(map[string]any)
	assert.Equal(t, "connected", comps["broker"])
	assert.Equal(t, "0/1 up", comps["sources"], "sources do not gate readiness")
	assert.Equal(t, "forwarding", comps["bridge"])

	require.NoError(t, f.broker.Close())
	code, body, _ = f.get(t, "/readyz")
	assert.Equal(t, 503, code)
	assert.Equal(t, "not ready", body["status"])
}

func TestAssetsAndLatest(t *testing.T) {
	f := newFixture(t, false)
	code, body, _ := f.get(t, "/v1/assets")
	require.Equal(t, 200, code)
	assert.Equal(t, "acme", body["enterprise"])
	assets := body["assets"].([]any)
	require.Len(t, assets, 1)
	a := assets[0].(map[string]any)
	assert.Equal(t, "filler-01", a["id"])
	assert.Equal(t, "RotaryFiller", a["class"])
	tags := a["tags"].([]any)
	require.Len(t, tags, 2)
	assert.Equal(t, "acme/austin/packaging/line-1/filling/speed", tags[0].(map[string]any)["topic"])

	code, body, _ = f.get(t, "/v1/assets/filler-01")
	assert.Equal(t, 200, code)
	assert.Equal(t, "packaging", body["area"])
	code, body, _ = f.get(t, "/v1/assets/nope")
	assert.Equal(t, 404, code)
	assert.Equal(t, "asset not found", body["error"])
	assert.NotEmpty(t, body["request_id"])

	// Latest: empty before data, populated after.
	code, body, _ = f.get(t, "/v1/tags/filler-01/latest")
	assert.Equal(t, 200, code)
	assert.Equal(t, float64(0), body["count"])

	f.latest.Set("filler-01", uns.Metric{Name: "speed", Value: 480.5, Quality: uns.QualityGood, Topic: "acme/austin/packaging/line-1/filling/speed",
		Timestamp: uns.Millis(time.Now()), DataType: uns.TypeFloat, Context: uns.Context{AssetID: "filler-01", Unit: "bpm"}})
	code, body, _ = f.get(t, "/v1/tags/filler-01/latest")
	assert.Equal(t, 200, code)
	assert.Equal(t, float64(1), body["count"])
	m := body["metrics"].([]any)[0].(map[string]any)
	assert.Equal(t, 480.5, m["value"])
	assert.Equal(t, "bpm", m["properties"].(map[string]any)["unit"])

	code, _, _ = f.get(t, "/v1/tags/nope/latest")
	assert.Equal(t, 404, code)
}

func TestTopicsAndSources(t *testing.T) {
	f := newFixture(t, false)
	f.latest.Set("filler-01", uns.Metric{Name: "speed", Quality: uns.QualityStale, Topic: "acme/austin/packaging/line-1/filling/speed", Timestamp: uns.Millis(time.Now())})

	code, body, _ := f.get(t, "/v1/topics")
	require.Equal(t, 200, code)
	assert.Equal(t, float64(2), body["count"])
	topics := body["topics"].([]any)
	first := topics[0].(map[string]any)
	assert.Equal(t, "acme/austin/packaging/line-1/filling/speed", first["topic"])
	assert.Equal(t, "STALE", first["quality"])
	assert.NotNil(t, first["last_seen"])
	assert.Nil(t, topics[1].(map[string]any)["last_seen"])
	assert.Len(t, body["lifecycle_topics"].([]any), 2)

	code, body, _ = f.get(t, "/v1/topics?filter=acme/austin/%2B/%2B/%2B/state")
	require.Equal(t, 200, code)
	assert.Equal(t, float64(1), body["count"])
	code, _, _ = f.get(t, "/v1/topics?filter=a/#/b")
	assert.Equal(t, 400, code)

	code, body, _ = f.get(t, "/v1/health/sources")
	require.Equal(t, 200, code)
	assert.Equal(t, float64(1), body["total"])
	assert.Equal(t, float64(0), body["up"])
	src := body["sources"].([]any)[0].(map[string]any)
	assert.Equal(t, "plc-1", src["name"])
	assert.Equal(t, false, src["up"])

	code, body, _ = f.get(t, "/v1/bridge")
	require.Equal(t, 200, code)
	assert.Equal(t, false, body["enabled"])

	code, _, _ = f.get(t, "/metrics")
	assert.Equal(t, 200, code)
	code, _, _ = f.get(t, "/nope")
	assert.Equal(t, 404, code)
}

func TestBridgeStatsEnabled(t *testing.T) {
	f := newFixture(t, true)
	code, body, _ := f.get(t, "/v1/bridge")
	require.Equal(t, 200, code)
	assert.Equal(t, true, body["enabled"])
	assert.Equal(t, true, body["stats"].(map[string]any)["sink_up"])
}

func TestStream_SSE(t *testing.T) {
	f := newFixture(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.srv.URL+"/v1/stream?topic=acme/austin/%23", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	rd := bufio.NewReader(resp.Body)
	line, err := rd.ReadString('\n')
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(line, ": subscribed"), line)

	// Wait for the subscription to be registered, then publish.
	require.Eventually(t, func() bool {
		return f.broker.Publish(context.Background(), ports.Message{Topic: "acme/austin/p/l/c/speed", Payload: []byte(`{"type":"DATA","node":"plc-1","metrics":[]}`)}) == nil
	}, time.Second, 5*time.Millisecond)
	// Non-JSON payloads are wrapped as strings; other namespaces are filtered out.
	require.NoError(t, f.broker.Publish(context.Background(), ports.Message{Topic: "acme/austin/p/l/c/raw", Payload: []byte("hello"), Retain: true}))
	require.NoError(t, f.broker.Publish(context.Background(), ports.Message{Topic: "other/x", Payload: []byte("nope")}))

	var events []map[string]any
	deadline := time.Now().Add(5 * time.Second)
	for len(events) < 2 && time.Now().Before(deadline) {
		line, err := rd.ReadString('\n')
		require.NoError(t, err)
		if strings.HasPrefix(line, "data: ") {
			var ev map[string]any
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev))
			events = append(events, ev)
		}
	}
	require.Len(t, events, 2)
	assert.Equal(t, "acme/austin/p/l/c/speed", events[0]["topic"])
	assert.Equal(t, "DATA", events[0]["payload"].(map[string]any)["type"])
	assert.Equal(t, "hello", events[1]["payload"])
	assert.Equal(t, true, events[1]["retain"])

	// Heartbeats keep flowing.
	for {
		line, err := rd.ReadString('\n')
		require.NoError(t, err)
		if strings.HasPrefix(line, ": heartbeat") {
			break
		}
	}
	cancel()
}

func TestStream_BadFilter(t *testing.T) {
	f := newFixture(t, false)
	code, body, _ := f.get(t, "/v1/stream?topic=a/%23/b")
	assert.Equal(t, 400, code)
	assert.Contains(t, body["error"], "#")
}

func TestStream_BrokerClosed(t *testing.T) {
	f := newFixture(t, false)
	require.NoError(t, f.broker.Close())
	code, _, _ := f.get(t, "/v1/stream")
	assert.Equal(t, 503, code)
}

func TestRequestIDPropagatesAndPanicRecovers(t *testing.T) {
	f := newFixture(t, false)
	req, _ := http.NewRequest(http.MethodGet, f.srv.URL+"/healthz", nil)
	req.Header.Set("X-Request-ID", "abc-123")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, "abc-123", resp.Header.Get("X-Request-ID"))

	s := New(Deps{Index: nil, Broker: f.broker})
	// A nil index makes /v1/assets panic; the recoverer turns it into a 500.
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/assets", nil))
	assert.Equal(t, 500, rec.Code)
	assert.Contains(t, rec.Body.String(), "internal error")

	// A nil index is also "not ready".
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, 503, rec.Code)
}
