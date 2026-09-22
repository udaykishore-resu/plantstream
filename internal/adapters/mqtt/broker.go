// Package mqtt implements ports.Broker on top of an MQTT v5 broker using
// eclipse/paho.golang's autopaho connection manager (automatic reconnect,
// re-subscription on reconnect, last-will support).
package mqtt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// Config configures the MQTT connection.
type Config struct {
	// URL of the broker, e.g. mqtt://localhost:1883 or tls://broker:8883.
	URL      string
	ClientID string
	Username string
	Password string
	// QoS used for publishes and subscriptions (0 or 1; default 1).
	QoS byte
	// KeepAlive in seconds (default 30).
	KeepAlive uint16
	// ConnectTimeout bounds the initial connection wait in New (default 10s).
	ConnectTimeout time.Duration
	// Will is an optional last-will message (typically the node DEATH).
	Will *ports.Message
}

type subscription struct {
	filter  string
	handler ports.Handler
	ctx     context.Context
}

// Broker is an MQTT-backed ports.Broker.
type Broker struct {
	cfg Config
	cm  *autopaho.ConnectionManager
	log *slog.Logger

	mu        sync.RWMutex
	subs      map[*subscription]struct{}
	connected atomic.Bool
	closed    atomic.Bool
}

// New connects to the broker and waits for the first successful connection
// (bounded by ConnectTimeout) so misconfiguration fails fast at startup.
func New(ctx context.Context, cfg Config, log *slog.Logger) (*Broker, error) {
	if log == nil {
		log = slog.Default()
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("mqtt: invalid url %q", cfg.URL)
	}
	if cfg.QoS > 1 {
		cfg.QoS = 1
	}
	if cfg.KeepAlive == 0 {
		cfg.KeepAlive = 30
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "plantstream"
	}
	b := &Broker{cfg: cfg, log: log, subs: map[*subscription]struct{}{}}

	ccfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     cfg.KeepAlive,
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         60,
		ConnectTimeout:                cfg.ConnectTimeout,
		ReconnectBackoff: func(attempt int) time.Duration {
			d := time.Duration(1<<min(attempt, 5)) * time.Second
			return min(d, 30*time.Second)
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			b.connected.Store(true)
			log.Info("mqtt connected", "url", cfg.URL, "client_id", cfg.ClientID)
			go b.resubscribe(cm)
		},
		OnConnectionDown: func() bool {
			b.connected.Store(false)
			log.Warn("mqtt connection lost; reconnecting")
			return true
		},
		OnConnectError: func(err error) { log.Warn("mqtt connect failed", "err", err) },
		ClientConfig: paho.ClientConfig{
			ClientID:          cfg.ClientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){b.onPublish},
			OnClientError:     func(err error) { log.Warn("mqtt client error", "err", err) },
		},
	}
	if cfg.Username != "" {
		ccfg.ConnectUsername = cfg.Username
		ccfg.ConnectPassword = []byte(cfg.Password)
	}
	if cfg.Will != nil {
		ccfg.SetWillMessage(cfg.Will.Topic, cfg.Will.Payload, cfg.QoS, cfg.Will.Retain)
	}
	cm, err := autopaho.NewConnection(ctx, ccfg)
	if err != nil {
		return nil, fmt.Errorf("mqtt: new connection: %w", err)
	}
	b.cm = cm
	wctx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := cm.AwaitConnection(wctx); err != nil {
		_ = cm.Disconnect(context.Background())
		return nil, fmt.Errorf("mqtt: connect to %s: %w", cfg.URL, err)
	}
	return b, nil
}

func (b *Broker) onPublish(pr paho.PublishReceived) (bool, error) {
	msg := ports.Message{Topic: pr.Packet.Topic, Payload: pr.Packet.Payload, Retain: pr.Packet.Retain, At: time.Now()}
	b.mu.RLock()
	targets := make([]*subscription, 0, len(b.subs))
	for s := range b.subs {
		if uns.MatchFilter(s.filter, msg.Topic) {
			targets = append(targets, s)
		}
	}
	b.mu.RUnlock()
	for _, s := range targets {
		if s.ctx.Err() == nil {
			s.handler(s.ctx, msg)
		}
	}
	return len(targets) > 0, nil
}

func (b *Broker) resubscribe(cm *autopaho.ConnectionManager) {
	b.mu.RLock()
	filters := make([]string, 0, len(b.subs))
	seen := map[string]bool{}
	for s := range b.subs {
		if !seen[s.filter] {
			seen[s.filter] = true
			filters = append(filters, s.filter)
		}
	}
	b.mu.RUnlock()
	if len(filters) == 0 {
		return
	}
	opts := make([]paho.SubscribeOptions, 0, len(filters))
	for _, f := range filters {
		opts = append(opts, paho.SubscribeOptions{Topic: f, QoS: b.cfg.QoS})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: opts}); err != nil {
		b.log.Warn("mqtt resubscribe failed", "err", err)
	}
}

// Publish implements ports.Broker.
func (b *Broker) Publish(ctx context.Context, msg ports.Message) error {
	if b.closed.Load() {
		return errors.New("mqtt: broker closed")
	}
	_, err := b.cm.Publish(ctx, &paho.Publish{
		Topic: msg.Topic, Payload: msg.Payload, QoS: b.cfg.QoS, Retain: msg.Retain,
	})
	if err != nil {
		return fmt.Errorf("mqtt: publish %s: %w", msg.Topic, err)
	}
	return nil
}

// Subscribe implements ports.Broker.
func (b *Broker) Subscribe(ctx context.Context, filter string, h ports.Handler) (func(), error) {
	if err := uns.ValidateFilter(filter); err != nil {
		return nil, err
	}
	if b.closed.Load() {
		return nil, errors.New("mqtt: broker closed")
	}
	s := &subscription{filter: filter, handler: h, ctx: ctx}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()

	if _, err := b.cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: b.cfg.QoS}}}); err != nil {
		b.mu.Lock()
		delete(b.subs, s)
		b.mu.Unlock()
		return nil, fmt.Errorf("mqtt: subscribe %s: %w", filter, err)
	}
	return func() {
		b.mu.Lock()
		delete(b.subs, s)
		still := false
		for o := range b.subs {
			if o.filter == filter {
				still = true
				break
			}
		}
		b.mu.Unlock()
		if !still && !b.closed.Load() {
			uctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = b.cm.Unsubscribe(uctx, &paho.Unsubscribe{Topics: []string{filter}})
		}
	}, nil
}

// Ready implements ports.Broker.
func (b *Broker) Ready() bool { return b.connected.Load() && !b.closed.Load() }

// Close disconnects cleanly (sending DISCONNECT, which suppresses the will).
func (b *Broker) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.cm.Disconnect(ctx); err != nil && !errors.Is(err, autopaho.ConnectionDownError) {
		return fmt.Errorf("mqtt: disconnect: %w", err)
	}
	return nil
}
