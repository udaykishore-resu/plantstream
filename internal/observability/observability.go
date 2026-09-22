// Package observability wires structured logging, OpenTelemetry tracing and
// Prometheus metrics. Metrics are defined once here so every package agrees
// on names and labels (RED for HTTP, plus domain metrics).
package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// NewLogger builds a JSON slog logger at the given level (debug|info|warn|error).
func NewLogger(w io.Writer, level string) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl}))
}

// TracingConfig configures the tracer provider.
type TracingConfig struct {
	ServiceName    string
	ServiceVersion string
	// Endpoint is the OTLP/HTTP collector endpoint (host:port). Empty disables export.
	Endpoint string
	Insecure bool
	// SampleRatio in [0,1]; 1 samples everything.
	SampleRatio float64
}

// SetupTracing installs a global tracer provider and W3C propagators. When no
// endpoint is configured spans are created but not exported, which keeps
// trace IDs in logs without requiring a collector.
func SetupTracing(ctx context.Context, cfg TracingConfig) (func(context.Context) error, error) {
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
	))
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}
	ratio := cfg.SampleRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 1
	}
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	}
	if cfg.Endpoint != "" {
		eopts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(cfg.Endpoint)}
		if cfg.Insecure {
			eopts = append(eopts, otlptracehttp.WithInsecure())
		}
		exp, err := otlptracehttp.New(ctx, eopts...)
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, nil
}

// Tracer returns the named tracer from the global provider.
func Tracer(name string) trace.Tracer { return otel.Tracer(name) }

// Metrics holds every Prometheus collector the service exposes.
type Metrics struct {
	registry *prometheus.Registry

	HTTPRequests  *prometheus.CounterVec
	HTTPDuration  *prometheus.HistogramVec
	HTTPInflight  prometheus.Gauge
	SSEClients    prometheus.Gauge
	SourceUp      *prometheus.GaugeVec
	SourcePolls   *prometheus.CounterVec
	PollDuration  *prometheus.HistogramVec
	Samples       *prometheus.CounterVec
	Published     *prometheus.CounterVec
	PublishErrors prometheus.Counter
	QualityTrans  *prometheus.CounterVec
	TagsByQuality *prometheus.GaugeVec

	BridgeForwarded prometheus.Counter
	BridgeSpilled   *prometheus.CounterVec
	BridgeDropped   prometheus.Counter
	BridgeCoalesced prometheus.Counter
	BridgeErrors    prometheus.Counter
	BridgeQueueLen  prometheus.Gauge
	BridgeQueueByte prometheus.Gauge
	BridgeEvicted   prometheus.Gauge
	BridgeSinkUp    prometheus.Gauge
	BridgeLag       prometheus.Histogram
}

// NewMetrics registers all collectors on a fresh registry (plus Go/process
// collectors) and returns them.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := promauto{reg}
	ns := "plantstream"
	m := &Metrics{
		registry: reg,
		HTTPRequests: f.counterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: "http", Name: "requests_total",
			Help: "HTTP requests by method, route and status class."}, "method", "route", "status"),
		HTTPDuration: f.histogramVec(prometheus.HistogramOpts{Namespace: ns, Subsystem: "http", Name: "request_duration_seconds",
			Help: "HTTP request latency.", Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}}, "method", "route"),
		HTTPInflight: f.gauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "http", Name: "inflight_requests", Help: "In-flight HTTP requests."}),
		SSEClients:   f.gauge(prometheus.GaugeOpts{Namespace: ns, Name: "sse_clients", Help: "Connected SSE stream clients."}),
		SourceUp: f.gaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: "source", Name: "up",
			Help: "1 when the last poll of the source succeeded."}, "source"),
		SourcePolls: f.counterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: "source", Name: "polls_total",
			Help: "Poll cycles by result (ok|error)."}, "source", "result"),
		PollDuration: f.histogramVec(prometheus.HistogramOpts{Namespace: ns, Subsystem: "source", Name: "poll_duration_seconds",
			Help: "Poll cycle latency.", Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}}, "source"),
		Samples: f.counterVec(prometheus.CounterOpts{Namespace: ns, Name: "samples_total",
			Help: "Tag samples collected."}, "source"),
		Published: f.counterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: "uns", Name: "published_total",
			Help: "Messages published to the UNS by type."}, "type"),
		PublishErrors: f.counter(prometheus.CounterOpts{Namespace: ns, Subsystem: "uns", Name: "publish_errors_total",
			Help: "Failed UNS publishes."}),
		QualityTrans: f.counterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: "quality", Name: "transitions_total",
			Help: "Quality code transitions by new code and the rule that fired."}, "quality", "rule"),
		TagsByQuality: f.gaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: "quality", Name: "tags",
			Help: "Number of tags currently carrying each quality code."}, "quality"),
		BridgeForwarded: f.counter(prometheus.CounterOpts{Namespace: ns, Subsystem: "bridge", Name: "forwarded_total",
			Help: "Messages delivered to the cloud sink."}),
		BridgeSpilled: f.counterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: "bridge", Name: "spilled_total",
			Help: "Messages written to the store-and-forward queue by reason."}, "reason"),
		BridgeDropped: f.counter(prometheus.CounterOpts{Namespace: ns, Subsystem: "bridge", Name: "dropped_total",
			Help: "Messages evicted from the bounded queue (data loss)."}),
		BridgeCoalesced: f.counter(prometheus.CounterOpts{Namespace: ns, Subsystem: "bridge", Name: "coalesced_total",
			Help: "Oldest in-memory samples dropped by per-topic backpressure (latest value kept)."}),
		BridgeErrors: f.counter(prometheus.CounterOpts{Namespace: ns, Subsystem: "bridge", Name: "sink_errors_total",
			Help: "Failed sink sends."}),
		BridgeEvicted: f.gauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "bridge", Name: "queue_evicted_total",
			Help: "Messages evicted from the bounded store-and-forward queue since start (data loss)."}),
		BridgeQueueLen:  f.gauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "bridge", Name: "queue_depth", Help: "Messages waiting in the store-and-forward queue."}),
		BridgeQueueByte: f.gauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "bridge", Name: "queue_bytes", Help: "Bytes occupied by the store-and-forward queue."}),
		BridgeSinkUp:    f.gauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "bridge", Name: "sink_up", Help: "1 when the cloud sink accepted the last send."}),
		BridgeLag: f.histogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: "bridge", Name: "delivery_lag_seconds",
			Help: "Time between message creation and sink acceptance.", Buckets: []float64{.01, .05, .1, .5, 1, 5, 30, 120, 600, 3600}}),
	}
	return m
}

// Registry exposes the registry (for tests and custom collectors).
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the /metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// promauto is a tiny registering factory bound to one registry.
type promauto struct{ reg prometheus.Registerer }

func (p promauto) counter(o prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(o)
	p.reg.MustRegister(c)
	return c
}

func (p promauto) counterVec(o prometheus.CounterOpts, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(o, labels)
	p.reg.MustRegister(c)
	return c
}

func (p promauto) gauge(o prometheus.GaugeOpts) prometheus.Gauge {
	g := prometheus.NewGauge(o)
	p.reg.MustRegister(g)
	return g
}

func (p promauto) gaugeVec(o prometheus.GaugeOpts, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(o, labels)
	p.reg.MustRegister(g)
	return g
}

func (p promauto) histogram(o prometheus.HistogramOpts) prometheus.Histogram {
	h := prometheus.NewHistogram(o)
	p.reg.MustRegister(h)
	return h
}

func (p promauto) histogramVec(o prometheus.HistogramOpts, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(o, labels)
	p.reg.MustRegister(h)
	return h
}
