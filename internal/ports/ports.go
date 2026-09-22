// Package ports defines the small interfaces through which the domain talks
// to external systems (MQTT broker, Kafka, on-disk queue). Every port has a
// production adapter and an in-memory adapter so the whole system runs with
// zero infrastructure in tests and `make run`.
package ports

import (
	"context"
	"time"
)

// Message is a topic-addressed byte payload, the unit of exchange on the UNS.
type Message struct {
	Topic   string
	Payload []byte
	// Retain asks the broker to keep the message as the topic's last value so
	// late subscribers receive it (BIRTH messages and latest values).
	Retain bool
	// At is when the message was created; used for ordering and metrics.
	At time.Time
}

// Handler receives subscribed messages. It must return quickly; adapters may
// invoke it from their network goroutine.
type Handler func(ctx context.Context, msg Message)

// Broker is the Unified Namespace transport.
type Broker interface {
	// Publish sends one message. It blocks until the broker accepted it or
	// ctx is done.
	Publish(ctx context.Context, msg Message) error
	// Subscribe registers a handler for an MQTT-style topic filter and
	// returns a function that cancels the subscription.
	Subscribe(ctx context.Context, filter string, h Handler) (func(), error)
	// Ready reports whether the broker connection is usable (for /readyz).
	Ready() bool
	Close() error
}

// Sink is the cloud-side destination of the edge→cloud bridge (Kafka).
type Sink interface {
	Send(ctx context.Context, msg Message) error
	Ready() bool
	Close() error
}

// Queue is a durable FIFO used for store-and-forward. Peek/Ack semantics let
// the bridge forward a message and only remove it after the sink accepted it.
type Queue interface {
	Push(msg Message) error
	// Peek returns the oldest message without removing it. ok is false when
	// the queue is empty.
	Peek() (msg Message, ok bool, err error)
	// Ack removes the oldest message (the one last returned by Peek).
	Ack() error
	Len() int
	Bytes() int64
	// Dropped counts messages evicted by the queue's bound (data loss).
	Dropped() int64
	Close() error
}
