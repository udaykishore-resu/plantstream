// Command plantstream is the edge node: it reads plant.yaml, polls the
// configured sources, publishes contextualised metrics to the Unified
// Namespace, optionally bridges them to Kafka and serves the read API.
//
// This file is wiring only: config → adapters → domain → http → run.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/udaykishore-resu/plantstream/internal/adapters/diskqueue"
	"github.com/udaykishore-resu/plantstream/internal/adapters/kafka"
	"github.com/udaykishore-resu/plantstream/internal/adapters/memory"
	"github.com/udaykishore-resu/plantstream/internal/adapters/mqtt"
	apihttp "github.com/udaykishore-resu/plantstream/internal/api/http"
	"github.com/udaykishore-resu/plantstream/internal/bridge"
	"github.com/udaykishore-resu/plantstream/internal/collectors"
	"github.com/udaykishore-resu/plantstream/internal/config"
	"github.com/udaykishore-resu/plantstream/internal/domain/asset"
	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
	"github.com/udaykishore-resu/plantstream/internal/edge"
	"github.com/udaykishore-resu/plantstream/internal/observability"
	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("plantstream exited with error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg.ServiceVersion == "dev" && version != "dev" {
		cfg.ServiceVersion = version
	}
	log := observability.NewLogger(os.Stdout, cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, observability.TracingConfig{
		ServiceName: "plantstream", ServiceVersion: cfg.ServiceVersion,
		Endpoint: cfg.OTelEndpoint, Insecure: cfg.OTelInsecure, SampleRatio: cfg.TraceSampleRatio,
	})
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = shutdownTracing(sctx)
	}()
	metrics := observability.NewMetrics()

	plant, err := asset.Load(cfg.PlantFile)
	if err != nil {
		return err
	}
	idx := plant.Index()
	log.Info("plant loaded", "file", cfg.PlantFile, "enterprise", plant.Enterprise, "site", plant.Site,
		"sources", len(plant.Sources), "assets", len(idx.Assets()), "tags", idx.TagCount())

	broker, err := newBroker(ctx, cfg, plant, log)
	if err != nil {
		return err
	}
	defer func() { _ = broker.Close() }() // best-effort shutdown; error is not actionable here

	health := collectors.NewHealth()
	node, err := edge.New(edge.Config{StaleCheckInterval: cfg.StaleCheck}, idx, broker, health, metrics, log.With("component", "edge"))
	if err != nil {
		return err
	}

	var br *bridge.Bridge
	var sink ports.Sink
	var queue ports.Queue
	if cfg.BridgeEnabled {
		sink, queue, err = newSinkAndQueue(ctx, cfg, log)
		if err != nil {
			return err
		}
		defer func() { _ = sink.Close() }()  // best-effort shutdown
		defer func() { _ = queue.Close() }() // best-effort shutdown
		br = bridge.New(bridge.Config{
			Filter: cfg.BridgeFilter, InboxSize: cfg.BridgeInboxSize, PerTopicMax: cfg.BridgePerTopicMax,
		}, broker, sink, queue, metrics, log.With("component", "bridge"))
	}

	api := apihttp.New(apihttp.Deps{
		Index: idx, Latest: node.Latest(), Health: health, Broker: broker, Bridge: br,
		Metrics: metrics, Log: log.With("component", "http"), Version: cfg.ServiceVersion,
	})
	srv := &http.Server{
		Addr: cfg.HTTPAddr, Handler: api, ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout: 60 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx },
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return node.Run(gctx) })
	if br != nil {
		g.Go(func() error { return br.Run(gctx) })
	}
	g.Go(func() error {
		log.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutdown requested; draining", "timeout", cfg.ShutdownTimeout)
		sctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		return srv.Shutdown(sctx)
	})
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("plantstream stopped")
	return nil
}

func newBroker(ctx context.Context, cfg config.Config, plant *asset.Plant, log *slog.Logger) (ports.Broker, error) {
	switch cfg.Broker {
	case "mqtt":
		clientID := cfg.MQTTClientID
		if clientID == "" {
			clientID = "plantstream-" + plant.Site
		}
		// Node-level last will: the broker publishes it if this process dies
		// without a clean DISCONNECT, complementing the per-source DEATHs.
		will, err := uns.Payload{Type: uns.Death, Timestamp: uns.Millis(time.Now()), Node: clientID}.Marshal()
		if err != nil {
			return nil, err
		}
		return mqtt.New(ctx, mqtt.Config{
			URL: cfg.MQTTURL, ClientID: clientID, Username: cfg.MQTTUsername, Password: cfg.MQTTPassword,
			QoS: cfg.MQTTQoS, KeepAlive: cfg.MQTTKeepAlive, ConnectTimeout: cfg.MQTTConnectTmo,
			Will: &ports.Message{Topic: uns.LifecycleTopic(plant.Enterprise, plant.Site, clientID, uns.Death), Payload: will},
		}, log.With("component", "mqtt"))
	default:
		log.Info("using in-memory broker (no external MQTT)")
		return memory.NewBroker(), nil
	}
}

func newSinkAndQueue(ctx context.Context, cfg config.Config, log *slog.Logger) (ports.Sink, ports.Queue, error) {
	var sink ports.Sink
	switch cfg.Sink {
	case "kafka":
		s, err := kafka.New(ctx, kafka.Config{Brokers: cfg.KafkaBrokers, Topic: cfg.KafkaTopic}, log.With("component", "kafka"))
		if err != nil {
			return nil, nil, err
		}
		sink = s
	default:
		log.Info("using in-memory sink (no external Kafka)")
		sink = memory.NewSink(10_000)
	}
	var queue ports.Queue
	if cfg.QueueDir != "" {
		q, err := diskqueue.Open(cfg.QueueDir, diskqueue.Options{MaxBytes: cfg.QueueMaxBytes, Sync: cfg.QueueSync})
		if err != nil {
			_ = sink.Close()
			return nil, nil, err
		}
		log.Info("store-and-forward queue opened", "dir", cfg.QueueDir, "max_bytes", cfg.QueueMaxBytes, "pending", q.Len())
		queue = q
	} else {
		log.Warn("store-and-forward queue is in-memory; set PLANTSTREAM_QUEUE_DIR for durability")
		queue = memory.NewQueue(cfg.QueueMaxBytes)
	}
	return sink, queue, nil
}
