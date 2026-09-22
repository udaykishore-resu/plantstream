// Package memory provides in-process implementations of the ports: a tiny
// pub/sub broker with MQTT wildcard semantics and retained messages, a
// recording sink and a bounded queue. They power tests and `make run`.
package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// ErrClosed is returned after Close.
var ErrClosed = errors.New("memory: broker closed")

const subscriberBuffer = 1024

type subscription struct {
	filter string
	ch     chan ports.Message
	done   chan struct{}
	once   sync.Once
}

func (s *subscription) close() { s.once.Do(func() { close(s.done) }) }

// Broker is an in-memory MQTT-like broker.
type Broker struct {
	mu       sync.RWMutex
	subs     map[*subscription]struct{}
	retained map[string]ports.Message
	closed   bool
	wg       sync.WaitGroup

	published atomic.Int64
}

// NewBroker creates an empty broker.
func NewBroker() *Broker {
	return &Broker{subs: map[*subscription]struct{}{}, retained: map[string]ports.Message{}}
}

// Publish delivers msg to every matching subscriber, blocking while a
// subscriber's buffer is full (natural backpressure) unless ctx ends.
func (b *Broker) Publish(ctx context.Context, msg ports.Message) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if msg.Retain {
		if len(msg.Payload) == 0 {
			delete(b.retained, msg.Topic)
		} else {
			b.retained[msg.Topic] = msg
		}
	}
	targets := make([]*subscription, 0, len(b.subs))
	for s := range b.subs {
		if uns.MatchFilter(s.filter, msg.Topic) {
			targets = append(targets, s)
		}
	}
	b.mu.Unlock()
	b.published.Add(1)

	for _, s := range targets {
		select {
		case s.ch <- msg:
		case <-s.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Subscribe registers h for filter. Retained messages matching the filter
// are delivered first. The handler runs on a dedicated goroutine per
// subscription so publishers are never blocked by handler latency beyond the
// buffer size.
func (b *Broker) Subscribe(ctx context.Context, filter string, h ports.Handler) (func(), error) {
	if err := uns.ValidateFilter(filter); err != nil {
		return nil, err
	}
	s := &subscription{filter: filter, ch: make(chan ports.Message, subscriberBuffer), done: make(chan struct{})}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	var backlog []ports.Message
	for topic, m := range b.retained {
		if uns.MatchFilter(filter, topic) {
			backlog = append(backlog, m)
		}
	}
	b.subs[s] = struct{}{}
	b.mu.Unlock()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for _, m := range backlog {
			h(ctx, m)
		}
		for {
			select {
			case m := <-s.ch:
				h(ctx, m)
			case <-s.done:
				return
			case <-ctx.Done():
				b.remove(s)
				return
			}
		}
	}()

	return func() { b.remove(s) }, nil
}

func (b *Broker) remove(s *subscription) {
	b.mu.Lock()
	delete(b.subs, s)
	b.mu.Unlock()
	s.close()
}

// Ready always reports true: the broker lives in-process.
func (b *Broker) Ready() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return !b.closed
}

// Published returns the number of messages published so far.
func (b *Broker) Published() int64 { return b.published.Load() }

// Retained returns the retained message for a topic, if any.
func (b *Broker) Retained(topic string) (ports.Message, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, ok := b.retained[topic]
	return m, ok
}

// Close stops all subscriptions and waits for their goroutines.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	subs := make([]*subscription, 0, len(b.subs))
	for s := range b.subs {
		subs = append(subs, s)
	}
	b.subs = map[*subscription]struct{}{}
	b.mu.Unlock()
	for _, s := range subs {
		s.close()
	}
	b.wg.Wait()
	return nil
}
