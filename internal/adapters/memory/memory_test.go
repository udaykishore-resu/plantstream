package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/udaykishore-resu/plantstream/internal/ports"
)

type collector struct {
	mu   sync.Mutex
	msgs []ports.Message
	got  chan struct{}
}

func newCollector() *collector { return &collector{got: make(chan struct{}, 1024)} }

func (c *collector) handle(_ context.Context, m ports.Message) {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
	c.got <- struct{}{}
}

func (c *collector) wait(t *testing.T, n int) []ports.Message {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-c.got:
		case <-deadline:
			t.Fatalf("timed out waiting for %d messages", n)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ports.Message, len(c.msgs))
	copy(out, c.msgs)
	return out
}

func TestBroker_WildcardsAndRetained(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()

	// Retained message published before anyone subscribes.
	require.NoError(t, b.Publish(ctx, ports.Message{Topic: "acme/austin/_edge/plc/BIRTH", Payload: []byte("birth"), Retain: true}))

	all := newCollector()
	unsubAll, err := b.Subscribe(ctx, "#", all.handle)
	require.NoError(t, err)

	speed := newCollector()
	_, err = b.Subscribe(ctx, "acme/austin/+/+/+/speed", speed.handle)
	require.NoError(t, err)

	// Late subscriber gets the retained BIRTH.
	got := all.wait(t, 1)
	assert.Equal(t, "birth", string(got[0].Payload))

	require.NoError(t, b.Publish(ctx, ports.Message{Topic: "acme/austin/pack/l1/c1/speed", Payload: []byte("1")}))
	require.NoError(t, b.Publish(ctx, ports.Message{Topic: "acme/austin/pack/l1/c1/temp", Payload: []byte("2")}))

	assert.Len(t, all.wait(t, 2), 3)
	sp := speed.wait(t, 1)
	require.Len(t, sp, 1)
	assert.Equal(t, "1", string(sp[0].Payload))

	assert.Equal(t, int64(3), b.Published())
	m, ok := b.Retained("acme/austin/_edge/plc/BIRTH")
	assert.True(t, ok)
	assert.Equal(t, "birth", string(m.Payload))

	// Clearing a retained message.
	require.NoError(t, b.Publish(ctx, ports.Message{Topic: "acme/austin/_edge/plc/BIRTH", Retain: true}))
	_, ok = b.Retained("acme/austin/_edge/plc/BIRTH")
	assert.False(t, ok)

	unsubAll()
	require.NoError(t, b.Publish(ctx, ports.Message{Topic: "acme/austin/pack/l1/c1/speed", Payload: []byte("3")}))
	assert.Len(t, speed.wait(t, 1), 2)
	time.Sleep(20 * time.Millisecond)
	all.mu.Lock()
	assert.Len(t, all.msgs, 4, "unsubscribed handler receives nothing more (birth, speed, temp, clear)")
	all.mu.Unlock()
}

func TestBroker_InvalidFilterAndClose(t *testing.T) {
	b := NewBroker()
	_, err := b.Subscribe(context.Background(), "a/#/b", func(context.Context, ports.Message) {})
	assert.Error(t, err)

	assert.True(t, b.Ready())
	require.NoError(t, b.Close())
	require.NoError(t, b.Close())
	assert.False(t, b.Ready())
	assert.ErrorIs(t, b.Publish(context.Background(), ports.Message{Topic: "x"}), ErrClosed)
	_, err = b.Subscribe(context.Background(), "#", func(context.Context, ports.Message) {})
	assert.ErrorIs(t, err, ErrClosed)
}

func TestBroker_SubscriptionEndsWithContext(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	c := newCollector()
	_, err := b.Subscribe(ctx, "#", c.handle)
	require.NoError(t, err)
	cancel()
	require.Eventually(t, func() bool {
		b.mu.RLock()
		defer b.mu.RUnlock()
		return len(b.subs) == 0
	}, time.Second, 5*time.Millisecond)
}

func TestBroker_PublishBlocksThenHonoursContext(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	block := make(chan struct{})
	_, err := b.Subscribe(context.Background(), "#", func(context.Context, ports.Message) { <-block })
	require.NoError(t, err)
	// Fill the buffer (+1 in the handler).
	for i := 0; i < subscriberBuffer+1; i++ {
		require.NoError(t, b.Publish(context.Background(), ports.Message{Topic: "t"}))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = b.Publish(ctx, ports.Message{Topic: "t"})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	close(block)
}

func TestSink(t *testing.T) {
	s := NewSink(2)
	ctx := context.Background()
	require.NoError(t, s.Send(ctx, ports.Message{Topic: "a"}))
	require.NoError(t, s.Send(ctx, ports.Message{Topic: "b"}))
	require.NoError(t, s.Send(ctx, ports.Message{Topic: "c"}))
	assert.Equal(t, 2, s.Len())
	assert.Equal(t, "b", s.Messages()[0].Topic)
	assert.True(t, s.Ready())

	s.SetFail(true)
	assert.False(t, s.Ready())
	assert.ErrorIs(t, s.Send(ctx, ports.Message{Topic: "d"}), ErrSinkDown)
	s.SetFail(false)

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	assert.ErrorIs(t, s.Send(cctx, ports.Message{Topic: "e"}), context.Canceled)

	require.NoError(t, s.Close())
	assert.ErrorIs(t, s.Send(ctx, ports.Message{Topic: "f"}), ErrClosed)
}

func TestQueue_BoundedDropOldest(t *testing.T) {
	q := NewQueue(30)
	for i := 0; i < 5; i++ {
		require.NoError(t, q.Push(ports.Message{Topic: "t", Payload: []byte("0123456789")})) // 11 bytes each
	}
	assert.Equal(t, 2, q.Len())
	assert.Equal(t, int64(22), q.Bytes())
	assert.Equal(t, int64(3), q.Dropped())

	m, ok, err := q.Peek()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "t", m.Topic)
	require.NoError(t, q.Ack())
	require.NoError(t, q.Ack())
	require.NoError(t, q.Ack()) // no-op on empty
	_, ok, _ = q.Peek()
	assert.False(t, ok)
	assert.Equal(t, int64(0), q.Bytes())
	assert.NoError(t, q.Close())
}
