// Package kafka implements ports.Sink with franz-go. Every UNS message becomes
// one Kafka record keyed by its topic, so all samples of a tag land in one
// partition in order and log-compacted topics keep the latest value per tag.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// Config configures the producer.
type Config struct {
	Brokers []string
	// Topic is the Kafka topic every UNS message is written to.
	Topic    string
	ClientID string
	// DeliveryTimeout bounds one Send (default 10s, minimum 1s). The bridge treats a
	// failure as "sink down" and switches to store-and-forward.
	DeliveryTimeout time.Duration
}

// Sink is a Kafka-backed ports.Sink.
type Sink struct {
	cl     *kgo.Client
	topic  string
	log    *slog.Logger
	ready  atomic.Bool
	closed atomic.Bool
}

// New creates the producer and pings the cluster once (bounded by ctx).
func New(ctx context.Context, cfg Config, log *slog.Logger) (*Sink, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: at least one broker is required")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka: topic is required")
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = 10 * time.Second
	}
	if cfg.DeliveryTimeout < time.Second {
		cfg.DeliveryTimeout = time.Second // franz-go's minimum
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "plantstream-bridge"
	}
	if log == nil {
		log = slog.Default()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression(), kgo.NoCompression()),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	s := &Sink{cl: cl, topic: cfg.Topic, log: log}
	if err := cl.Ping(ctx); err != nil {
		// Not fatal: the bridge will store-and-forward until Kafka is up.
		log.Warn("kafka not reachable at startup; bridge will buffer", "brokers", cfg.Brokers, "err", err)
	} else {
		s.ready.Store(true)
		log.Info("kafka connected", "brokers", cfg.Brokers, "topic", cfg.Topic)
	}
	return s, nil
}

// Send implements ports.Sink: a synchronous, acknowledged produce.
func (s *Sink) Send(ctx context.Context, msg ports.Message) error {
	if s.closed.Load() {
		return errors.New("kafka: sink closed")
	}
	rec := &kgo.Record{
		Key:   []byte(msg.Topic),
		Value: msg.Payload,
		Headers: []kgo.RecordHeader{
			{Key: "uns-topic", Value: []byte(msg.Topic)},
			{Key: "edge-ts-ms", Value: []byte(strconv.FormatInt(msg.At.UnixMilli(), 10))},
		},
	}
	if !msg.At.IsZero() {
		rec.Timestamp = msg.At
	}
	if err := s.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		s.ready.Store(false)
		return fmt.Errorf("kafka: produce %s: %w", msg.Topic, err)
	}
	s.ready.Store(true)
	return nil
}

// Ready implements ports.Sink.
func (s *Sink) Ready() bool { return s.ready.Load() && !s.closed.Load() }

// Close flushes and closes the client.
func (s *Sink) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.cl.Flush(ctx)
	s.cl.Close()
	return err
}
