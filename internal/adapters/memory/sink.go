package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// ErrSinkDown is returned by Sink.Send while the sink is set to fail.
var ErrSinkDown = errors.New("memory: sink unavailable")

// Sink records every message it receives. Tests flip Fail to simulate a
// cloud outage and exercise store-and-forward.
type Sink struct {
	mu       sync.Mutex
	messages []ports.Message
	fail     bool
	closed   bool
	max      int
}

// NewSink creates a sink that keeps at most max messages (0 = unbounded).
func NewSink(max int) *Sink { return &Sink{max: max} }

// Send implements ports.Sink.
func (s *Sink) Send(ctx context.Context, msg ports.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.fail {
		return ErrSinkDown
	}
	s.messages = append(s.messages, msg)
	if s.max > 0 && len(s.messages) > s.max {
		s.messages = s.messages[len(s.messages)-s.max:]
	}
	return nil
}

// SetFail toggles failure mode.
func (s *Sink) SetFail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

// Ready implements ports.Sink.
func (s *Sink) Ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.fail && !s.closed
}

// Messages returns a copy of everything received.
func (s *Sink) Messages() []ports.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ports.Message, len(s.messages))
	copy(out, s.messages)
	return out
}

// Len returns the number of stored messages.
func (s *Sink) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messages)
}

// Close implements ports.Sink.
func (s *Sink) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// Queue is an in-memory ports.Queue bounded by bytes with drop-oldest
// semantics, mirroring the on-disk implementation for tests.
type Queue struct {
	mu       sync.Mutex
	items    []ports.Message
	bytes    int64
	maxBytes int64
	dropped  int64
}

// NewQueue creates a queue holding at most maxBytes of payload (0 = unbounded).
func NewQueue(maxBytes int64) *Queue { return &Queue{maxBytes: maxBytes} }

// Push implements ports.Queue.
func (q *Queue) Push(msg ports.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	size := int64(len(msg.Payload) + len(msg.Topic))
	q.items = append(q.items, msg)
	q.bytes += size
	for q.maxBytes > 0 && q.bytes > q.maxBytes && len(q.items) > 1 {
		old := q.items[0]
		q.items = q.items[1:]
		q.bytes -= int64(len(old.Payload) + len(old.Topic))
		q.dropped++
	}
	return nil
}

// Peek implements ports.Queue.
func (q *Queue) Peek() (ports.Message, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return ports.Message{}, false, nil
	}
	return q.items[0], true, nil
}

// Ack implements ports.Queue.
func (q *Queue) Ack() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	old := q.items[0]
	q.items = q.items[1:]
	q.bytes -= int64(len(old.Payload) + len(old.Topic))
	return nil
}

// Len implements ports.Queue.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Bytes implements ports.Queue.
func (q *Queue) Bytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes
}

// Dropped returns how many messages were evicted by the bound.
func (q *Queue) Dropped() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}

// Close implements ports.Queue.
func (q *Queue) Close() error { return nil }
