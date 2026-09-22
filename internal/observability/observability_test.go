package observability

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLogger_LevelsAndJSON(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "warn")
	log.Info("hidden")
	log.Warn("shown", "k", "v")
	assert.NotContains(t, buf.String(), "hidden")
	assert.Contains(t, buf.String(), `"msg":"shown"`)
	assert.Contains(t, buf.String(), `"k":"v"`)

	for _, lvl := range []string{"debug", "info", "error", "warning", "bogus"} {
		assert.NotNil(t, NewLogger(nil, lvl))
	}
}

func TestSetupTracing_NoExporter(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), TracingConfig{ServiceName: "t", ServiceVersion: "v", SampleRatio: 2})
	require.NoError(t, err)
	_, span := Tracer("test").Start(context.Background(), "op")
	assert.True(t, span.SpanContext().IsValid())
	span.End()
	require.NoError(t, shutdown(context.Background()))
}

func TestSetupTracing_WithEndpoint(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), TracingConfig{ServiceName: "t", Endpoint: "127.0.0.1:1", Insecure: true, SampleRatio: 0.5})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = shutdown(ctx)
}

func TestMetrics_Exposed(t *testing.T) {
	m := NewMetrics()
	m.HTTPRequests.WithLabelValues("GET", "/x", "2xx").Inc()
	m.Samples.WithLabelValues("plc").Add(3)
	m.BridgeSpilled.WithLabelValues("sink_down").Inc()
	m.TagsByQuality.WithLabelValues("GOOD").Set(4)
	m.QualityTrans.WithLabelValues("STALE", "quality.stale@v1").Inc()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"plantstream_http_requests_total", "plantstream_samples_total{source=\"plc\"} 3",
		"plantstream_bridge_spilled_total{reason=\"sink_down\"} 1", "plantstream_quality_tags{quality=\"GOOD\"} 4",
		"plantstream_quality_transitions_total", "go_goroutines",
	} {
		assert.True(t, strings.Contains(body, want), "missing %s", want)
	}
	assert.NotNil(t, m.Registry())
}
