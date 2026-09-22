// Package http exposes the edge node's read API: asset model, latest values,
// topics, source health and a Server-Sent Events stream of the UNS.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/udaykishore-resu/plantstream/internal/bridge"
	"github.com/udaykishore-resu/plantstream/internal/collectors"
	"github.com/udaykishore-resu/plantstream/internal/domain/asset"
	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
	"github.com/udaykishore-resu/plantstream/internal/edge"
	"github.com/udaykishore-resu/plantstream/internal/observability"
	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// Deps are the collaborators the handlers read from.
type Deps struct {
	Index   *asset.Index
	Latest  *edge.Latest
	Health  *collectors.Health
	Broker  ports.Broker
	Bridge  *bridge.Bridge // optional
	Metrics *observability.Metrics
	Log     *slog.Logger
	Version string
	// SSEBuffer is the per-client buffer before slow clients lose messages (default 256).
	SSEBuffer int
	// Heartbeat interval for SSE comments (default 15s).
	Heartbeat time.Duration
}

// Server is the HTTP API.
type Server struct {
	deps Deps
	mux  *http.ServeMux
	h    http.Handler
}

// New builds the router with middleware.
func New(d Deps) *Server {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Metrics == nil {
		d.Metrics = observability.NewMetrics()
	}
	if d.SSEBuffer <= 0 {
		d.SSEBuffer = 256
	}
	if d.Heartbeat <= 0 {
		d.Heartbeat = 15 * time.Second
	}
	s := &Server{deps: d, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.HandleFunc("GET /readyz", s.readyz)
	s.mux.Handle("GET /metrics", d.Metrics.Handler())
	s.mux.HandleFunc("GET /v1/assets", s.assets)
	s.mux.HandleFunc("GET /v1/assets/{id}", s.asset)
	s.mux.HandleFunc("GET /v1/tags/{asset}/latest", s.latest)
	s.mux.HandleFunc("GET /v1/topics", s.topics)
	s.mux.HandleFunc("GET /v1/health/sources", s.sources)
	s.mux.HandleFunc("GET /v1/bridge", s.bridgeStats)
	s.mux.HandleFunc("GET /v1/stream", s.stream)
	s.h = s.recoverer(s.requestID(s.instrument(s.mux)))
	return s
}

// Handler returns the fully wrapped handler.
func (s *Server) Handler() http.Handler { return s.h }

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.h.ServeHTTP(w, r) }

// ---- responses -----------------------------------------------------------

type errorBody struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg, RequestID: requestIDFrom(r.Context())})
}

// ---- handlers ------------------------------------------------------------

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": s.deps.Version})
}

type readiness struct {
	Status     string            `json:"status"`
	Components map[string]string `json:"components"`
}

func (s *Server) readyz(w http.ResponseWriter, _ *http.Request) {
	r := readiness{Status: "ready", Components: map[string]string{}}
	ok := true
	if s.deps.Index == nil || s.deps.Index.TagCount() == 0 {
		r.Components["plant"] = "no tags loaded"
		ok = false
	} else {
		r.Components["plant"] = fmt.Sprintf("%d assets, %d tags", len(s.deps.Index.Assets()), s.deps.Index.TagCount())
	}
	if s.deps.Broker == nil || !s.deps.Broker.Ready() {
		r.Components["broker"] = "not connected"
		ok = false
	} else {
		r.Components["broker"] = "connected"
	}
	if s.deps.Health != nil {
		up, total := 0, 0
		for _, st := range s.deps.Health.Snapshot() {
			total++
			if st.Up {
				up++
			}
		}
		// Sources are reported but do not gate readiness: a PLC outage is a
		// data-quality event (BAD), not a reason to stop serving the API.
		r.Components["sources"] = fmt.Sprintf("%d/%d up", up, total)
	}
	if s.deps.Bridge != nil {
		// Store-and-forward makes a sink outage survivable; report only.
		if s.deps.Bridge.Ready() {
			r.Components["bridge"] = "forwarding"
		} else {
			r.Components["bridge"] = "sink down, buffering"
		}
	}
	if !ok {
		r.Status = "not ready"
		writeJSON(w, http.StatusServiceUnavailable, r)
		return
	}
	writeJSON(w, http.StatusOK, r)
}

type tagView struct {
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Unit        string       `json:"unit,omitempty"`
	DataType    uns.DataType `json:"datatype"`
	Range       *asset.Range `json:"range,omitempty"`
	Topic       string       `json:"topic"`
}

type assetView struct {
	ID          string    `json:"id"`
	Class       string    `json:"class"`
	Description string    `json:"description,omitempty"`
	Source      string    `json:"source"`
	Area        string    `json:"area"`
	Line        string    `json:"line"`
	Cell        string    `json:"cell"`
	Tags        []tagView `json:"tags"`
}

func (s *Server) assetView(ref asset.AssetRef) assetView {
	v := assetView{
		ID: ref.Asset.ID, Class: ref.Asset.Class, Description: ref.Asset.Description, Source: ref.Asset.Source,
		Area: ref.Area, Line: ref.Line, Cell: ref.Cell, Tags: make([]tagView, 0, len(ref.Asset.Tags)),
	}
	p := s.deps.Index.Plant()
	for i := range ref.Asset.Tags {
		t := &ref.Asset.Tags[i]
		topic := uns.Topic{Enterprise: p.Enterprise, Site: p.Site, Area: ref.Area, Line: ref.Line, Cell: ref.Cell, Tag: t.Name}
		v.Tags = append(v.Tags, tagView{Name: t.Name, Description: t.Description, Unit: t.Unit, DataType: t.DataType, Range: t.Range, Topic: topic.String()})
	}
	return v
}

func (s *Server) assets(w http.ResponseWriter, _ *http.Request) {
	refs := s.deps.Index.Assets()
	out := make([]assetView, 0, len(refs))
	for _, ref := range refs {
		out = append(out, s.assetView(ref))
	}
	p := s.deps.Index.Plant()
	writeJSON(w, http.StatusOK, map[string]any{"enterprise": p.Enterprise, "site": p.Site, "assets": out})
}

func (s *Server) asset(w http.ResponseWriter, r *http.Request) {
	ref, ok := s.deps.Index.Asset(r.PathValue("id"))
	if !ok {
		writeError(w, r, http.StatusNotFound, "asset not found")
		return
	}
	writeJSON(w, http.StatusOK, s.assetView(ref))
}

func (s *Server) latest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("asset")
	ref, ok := s.deps.Index.Asset(id)
	if !ok {
		writeError(w, r, http.StatusNotFound, "asset not found")
		return
	}
	metrics, _ := s.deps.Latest.Asset(id)
	if metrics == nil {
		metrics = []uns.Metric{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset_id": ref.Asset.ID, "class": ref.Asset.Class, "source": ref.Asset.Source,
		"count": len(metrics), "metrics": metrics,
	})
}

type topicView struct {
	Topic    string      `json:"topic"`
	AssetID  string      `json:"asset_id"`
	Tag      string      `json:"tag"`
	Source   string      `json:"source"`
	Quality  uns.Quality `json:"quality,omitempty"`
	LastSeen *uns.Millis `json:"last_seen,omitempty"`
}

func (s *Server) topics(w http.ResponseWriter, r *http.Request) {
	filter := r.URL.Query().Get("filter")
	if filter != "" {
		if err := uns.ValidateFilter(filter); err != nil {
			writeError(w, r, http.StatusBadRequest, err.Error())
			return
		}
	}
	p := s.deps.Index.Plant()
	out := []topicView{}
	for _, t := range s.deps.Index.Topics() {
		if filter != "" && !uns.MatchFilter(filter, t) {
			continue
		}
		ref, _ := s.deps.Index.TagByTopic(t)
		v := topicView{Topic: t, AssetID: ref.Asset.ID, Tag: ref.Tag.Name, Source: ref.Asset.Source}
		if m, ok := s.deps.Latest.Topic(t); ok {
			v.Quality = m.Quality
			ts := m.Timestamp
			v.LastSeen = &ts
		}
		out = append(out, v)
	}
	lifecycle := make([]string, 0, len(p.Sources)*2)
	for _, src := range p.Sources {
		lifecycle = append(lifecycle, uns.LifecycleTopic(p.Enterprise, p.Site, src.Name, uns.Birth), uns.LifecycleTopic(p.Enterprise, p.Site, src.Name, uns.Death))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": p.Enterprise + "/" + p.Site, "count": len(out), "topics": out, "lifecycle_topics": lifecycle,
	})
}

func (s *Server) sources(w http.ResponseWriter, _ *http.Request) {
	snap := s.deps.Health.Snapshot()
	up := 0
	for _, st := range snap {
		if st.Up {
			up++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"up": up, "total": len(snap), "sources": snap})
}

func (s *Server) bridgeStats(w http.ResponseWriter, r *http.Request) {
	if s.deps.Bridge == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"enabled": true, "stats": s.deps.Bridge.Stats()})
}

// stream serves Server-Sent Events for every UNS message matching ?topic=.
// Each event is {"topic": ..., "payload": <UNS payload>}. Slow clients lose
// messages rather than stalling publishers; the drop count is sent as a
// comment so the client knows.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	filter := r.URL.Query().Get("topic")
	if filter == "" {
		filter = "#"
	}
	if err := uns.ValidateFilter(filter); err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, r, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	ctx := r.Context()
	ch := make(chan ports.Message, s.deps.SSEBuffer)
	var dropped atomic.Int64
	unsub, err := s.deps.Broker.Subscribe(ctx, filter, func(_ context.Context, m ports.Message) {
		select {
		case ch <- m:
		default:
			dropped.Add(1)
		}
	})
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer unsub()

	s.deps.Metrics.SSEClients.Inc()
	defer s.deps.Metrics.SSEClients.Dec()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	// The filter is not echoed back: it is caller-supplied input and the client already knows it.
	_, _ = io.WriteString(w, ": subscribed\n\n") // client disconnects surface via ctx.Done()
	flusher.Flush()

	hb := time.NewTicker(s.deps.Heartbeat)
	defer hb.Stop()
	var id uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-hb.C:
			_, _ = fmt.Fprintf(w, ": heartbeat dropped=%d\n\n", dropped.Load()) // client disconnects surface via ctx.Done()
			flusher.Flush()
		case m := <-ch:
			id++
			payload := json.RawMessage(m.Payload)
			if !json.Valid(m.Payload) {
				b, _ := json.Marshal(string(m.Payload))
				payload = b
			}
			body, err := json.Marshal(struct {
				Topic   string          `json:"topic"`
				Retain  bool            `json:"retain,omitempty"`
				Payload json.RawMessage `json:"payload"`
			}{m.Topic, m.Retain, payload})
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "id: %d\nevent: message\ndata: %s\n\n", id, body) // client disconnects surface via ctx.Done()
			flusher.Flush()
		}
	}
}

// ---- middleware ----------------------------------------------------------

type ctxKey int

const requestIDKey ctxKey = iota

func requestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

// Flush keeps SSE working through the recorder.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		s.deps.Metrics.HTTPInflight.Inc()
		next.ServeHTTP(rec, r)
		s.deps.Metrics.HTTPInflight.Dec()
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		dur := time.Since(start)
		s.deps.Metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status/100)+"xx").Inc()
		s.deps.Metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(dur.Seconds())
		attrs := []any{
			"method", r.Method, "path", r.URL.Path, "route", route, "status", rec.status,
			"duration_ms", dur.Milliseconds(), "request_id", requestIDFrom(r.Context()), "remote", r.RemoteAddr,
		}
		if sc := trace.SpanContextFromContext(r.Context()); sc.IsValid() {
			attrs = append(attrs, "trace_id", sc.TraceID().String())
		}
		if rec.status >= 500 {
			s.deps.Log.Error("http request", attrs...)
		} else {
			s.deps.Log.Info("http request", attrs...)
		}
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(rec)
				}
				s.deps.Log.Error("panic in handler", "panic", rec, "path", r.URL.Path)
				writeError(w, r, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
