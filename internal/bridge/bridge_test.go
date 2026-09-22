package bridge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/udaykishore-resu/plantstream/internal/adapters/diskqueue"
	"github.com/udaykishore-resu/plantstream/internal/adapters/memory"
	"github.com/udaykishore-resu/plantstream/internal/ports"
)

func msg(topic string, i int) ports.Message {
	return ports.Message{Topic: topic, Payload: []byte(fmt.Sprintf("%d", i)), At: time.Now()}
}

func run(t *testing.T, b *Bridge) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	select {
	case <-b.Started():
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not start")
	}
	return func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("bridge did not stop")
		}
	}
}

func TestBridge_ForwardsLiveMessages(t *testing.T) {
	broker := memory.NewBroker()
	defer broker.Close()
	sink := memory.NewSink(0)
	queue := memory.NewQueue(0)
	b := New(Config{Filter: "acme/#"}, broker, sink, queue, nil, nil)
	stop := run(t, b)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		require.NoError(t, broker.Publish(ctx, msg("acme/austin/a/l/c/speed", i)))
	}
	require.NoError(t, broker.Publish(ctx, msg("other/site", 0)), "not bridged")

	require.Eventually(t, func() bool { return sink.Len() == 10 }, 3*time.Second, 5*time.Millisecond)
	for i, m := range sink.Messages() {
		assert.Equal(t, fmt.Sprint(i), string(m.Payload), "order preserved")
	}
	st := b.Stats()
	assert.True(t, st.SinkUp)
	assert.Equal(t, int64(10), st.Forwarded)
	assert.Equal(t, int64(0), st.Spilled)
	assert.Equal(t, 0, st.QueueDepth)
	assert.True(t, b.Ready())
	stop()
}

func TestBridge_StoreAndForwardAcrossOutage(t *testing.T) {
	broker := memory.NewBroker()
	defer broker.Close()
	sink := memory.NewSink(0)
	queue, err := diskqueue.Open(t.TempDir(), diskqueue.Options{})
	require.NoError(t, err)
	defer queue.Close()
	b := New(Config{RetryMin: 20 * time.Millisecond, RetryMax: 50 * time.Millisecond}, broker, sink, queue, nil, nil)
	stop := run(t, b)

	ctx := context.Background()
	require.NoError(t, broker.Publish(ctx, msg("t/1", 0)))
	require.Eventually(t, func() bool { return sink.Len() == 1 }, 3*time.Second, 5*time.Millisecond)

	// Outage: everything is stored on disk, in order.
	sink.SetFail(true)
	for i := 1; i <= 50; i++ {
		require.NoError(t, broker.Publish(ctx, msg("t/1", i)))
	}
	require.Eventually(t, func() bool {
		st := b.Stats()
		return !st.SinkUp && st.QueueDepth == 50
	}, 3*time.Second, 5*time.Millisecond, "stats: %+v", b.Stats())
	assert.Equal(t, 1, sink.Len())
	assert.False(t, b.Ready())

	// Recovery: replay drains the queue and live traffic resumes behind it.
	sink.SetFail(false)
	require.Eventually(t, func() bool { return sink.Len() == 51 }, 5*time.Second, 5*time.Millisecond)
	for i, m := range sink.Messages() {
		assert.Equal(t, fmt.Sprint(i), string(m.Payload), "message %d out of order", i)
	}
	require.Eventually(t, func() bool { return b.Stats().SinkUp && b.Stats().QueueDepth == 0 }, 3*time.Second, 5*time.Millisecond)

	require.NoError(t, broker.Publish(ctx, msg("t/1", 51)))
	require.Eventually(t, func() bool { return sink.Len() == 52 }, 3*time.Second, 5*time.Millisecond)
	stop()
}

func TestBridge_PerTopicBackpressureCoalesces(t *testing.T) {
	broker := memory.NewBroker()
	defer broker.Close()
	sink := memory.NewSink(0)
	queue := memory.NewQueue(0)
	b := New(Config{PerTopicMax: 2, InboxSize: 1000}, broker, sink, queue, nil, nil)

	// Drive the callback directly: no forwarder is draining the inbox.
	for i := 0; i < 5; i++ {
		b.onMessage(context.Background(), msg("chatty", i))
	}
	b.onMessage(context.Background(), msg("quiet", 0))
	st := b.Stats()
	assert.Equal(t, 3, st.Inbox, "2 newest chatty + 1 quiet in memory")
	assert.Equal(t, int64(3), st.Coalesced, "oldest chatty samples dropped")
	assert.Equal(t, 0, queue.Len(), "backpressure never touches the disk queue")

	// Round-robin dequeue: quiet is not starved by chatty, order per topic kept.
	m, ok := b.in.pop()
	require.True(t, ok)
	assert.Equal(t, "chatty", m.Topic)
	assert.Equal(t, "3", string(m.Payload))
	m, _ = b.in.pop()
	assert.Equal(t, "quiet", m.Topic)
	m, _ = b.in.pop()
	assert.Equal(t, "chatty", m.Topic)
	assert.Equal(t, "4", string(m.Payload))
	_, ok = b.in.pop()
	assert.False(t, ok)

	// Global budget evicts from the longest FIFO.
	small := New(Config{PerTopicMax: 10, InboxSize: 3}, broker, sink, memory.NewQueue(0), nil, nil)
	small.onMessage(context.Background(), msg("a", 0))
	small.onMessage(context.Background(), msg("a", 1))
	small.onMessage(context.Background(), msg("b", 0))
	small.onMessage(context.Background(), msg("c", 0))
	assert.Equal(t, 3, small.Stats().Inbox)
	assert.Equal(t, int64(1), small.Stats().Coalesced)
	m, _ = small.in.pop()
	assert.Equal(t, "1", string(m.Payload), "a's oldest was evicted")
}

func TestInbox_RingBookkeeping(t *testing.T) {
	in := newInbox(1, 100)
	in.push(msg("a", 0))
	in.push(msg("b", 0))
	in.push(msg("c", 0))
	// Evict a topic in the middle of the ring via the global budget path.
	in.maxTotal = 3
	in.push(msg("d", 0)) // evicts longest (all len 1 → first: a)
	assert.Equal(t, 3, in.len())
	var got []string
	for {
		m, ok := in.pop()
		if !ok {
			break
		}
		got = append(got, m.Topic)
	}
	assert.ElementsMatch(t, []string{"b", "c", "d"}, got)
	assert.False(t, in.evictLongest())
	_, ok := in.pop()
	assert.False(t, ok)
}

func TestBridge_DrainsInboxOnShutdown(t *testing.T) {
	broker := memory.NewBroker()
	defer broker.Close()
	sink := memory.NewSink(0)
	sink.SetFail(true)
	queue := memory.NewQueue(0)
	b := New(Config{RetryMin: time.Hour, RetryMax: time.Hour}, broker, sink, queue, nil, nil)
	// Pre-load the inbox without a forwarder running.
	for i := 0; i < 5; i++ {
		b.onMessage(context.Background(), msg("t", i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, b.Run(ctx))
	assert.Equal(t, 5, queue.Len(), "in-flight messages persisted on shutdown")
	assert.Equal(t, 0, b.Stats().Inbox)
}

func TestBridge_SubscribeErrorAndClosedBroker(t *testing.T) {
	broker := memory.NewBroker()
	b := New(Config{Filter: "bad/#/x"}, broker, memory.NewSink(0), memory.NewQueue(0), nil, nil)
	assert.Error(t, b.Run(context.Background()))
	require.NoError(t, broker.Close())
	b = New(Config{}, broker, memory.NewSink(0), memory.NewQueue(0), nil, nil)
	assert.Error(t, b.Run(context.Background()))
}

func TestBridge_ReplaysQueueLeftFromPreviousRun(t *testing.T) {
	broker := memory.NewBroker()
	defer broker.Close()
	sink := memory.NewSink(0)
	queue := memory.NewQueue(0)
	for i := 0; i < 3; i++ {
		require.NoError(t, queue.Push(msg("old", i)))
	}
	b := New(Config{ReplayBatch: 1}, broker, sink, queue, nil, nil)
	stop := run(t, b)
	require.Eventually(t, func() bool { return sink.Len() == 3 }, 3*time.Second, 5*time.Millisecond)
	stop()
}

func TestBridge_BoundedQueueDropsOldestUnderLongOutage(t *testing.T) {
	broker := memory.NewBroker()
	defer broker.Close()
	sink := memory.NewSink(0)
	sink.SetFail(true)
	queue := memory.NewQueue(200) // tiny budget
	b := New(Config{RetryMin: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond, PerTopicMax: 1000}, broker, sink, queue, nil, nil)
	stop := run(t, b)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		require.NoError(t, broker.Publish(ctx, msg("t", i)))
	}
	if !assert.Eventually(t, func() bool { return b.Stats().Dropped > 0 && b.Stats().Inbox == 0 }, 3*time.Second, 5*time.Millisecond) {
		t.Fatalf("stats %+v", b.Stats())
	}
	sink.SetFail(false)
	require.Eventually(t, func() bool { return b.Stats().QueueDepth == 0 && sink.Len() > 0 }, 3*time.Second, 5*time.Millisecond)
	// Newest message always survives the bound.
	msgs := sink.Messages()
	assert.Equal(t, "99", string(msgs[len(msgs)-1].Payload))
	stop()
}

func TestConfigDefaults(t *testing.T) {
	var c Config
	c.defaults()
	assert.Equal(t, "#", c.Filter)
	assert.Equal(t, 4096, c.InboxSize)
	assert.Equal(t, 64, c.PerTopicMax)
	c = Config{InboxSize: 10, PerTopicMax: 50}
	c.defaults()
	assert.Equal(t, 10, c.PerTopicMax)
	assert.Equal(t, 200*time.Millisecond, c.RetryMin)
	assert.Equal(t, 10*time.Second, c.RetryMax)
	assert.Equal(t, 256, c.ReplayBatch)
	c = Config{RetryMin: time.Minute, RetryMax: time.Second}
	c.defaults()
	assert.Equal(t, 10*time.Second, c.RetryMax)
}
