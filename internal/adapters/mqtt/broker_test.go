package mqtt

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// Integration against a real MQTT broker lives in docker-compose (make
// run-full); unit tests cover configuration handling and message routing.

func TestNew_InvalidURL(t *testing.T) {
	_, err := New(context.Background(), Config{URL: "::not a url"}, nil)
	assert.Error(t, err)
	_, err = New(context.Background(), Config{URL: "mqtt://"}, nil)
	assert.Error(t, err)
}

func TestNew_UnreachableFailsFast(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	start := time.Now()
	_, err = New(context.Background(), Config{URL: "mqtt://" + addr, ConnectTimeout: 300 * time.Millisecond}, nil)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Contains(t, err.Error(), "connect to")
}

func TestOnPublish_RoutesByFilter(t *testing.T) {
	b := &Broker{subs: map[*subscription]struct{}{}}
	var got []string
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	b.subs[&subscription{filter: "acme/+/speed", ctx: ctx, handler: func(_ context.Context, m ports.Message) { got = append(got, "a:"+m.Topic) }}] = struct{}{}
	b.subs[&subscription{filter: "acme/#", ctx: cancelled, handler: func(_ context.Context, m ports.Message) { got = append(got, "dead:"+m.Topic) }}] = struct{}{}
	b.subs[&subscription{filter: "other/#", ctx: ctx, handler: func(_ context.Context, m ports.Message) { got = append(got, "o:"+m.Topic) }}] = struct{}{}

	handled, err := b.onPublish(paho.PublishReceived{Packet: &paho.Publish{Topic: "acme/l1/speed", Payload: []byte("1"), Retain: true}})
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, []string{"a:acme/l1/speed"}, got, "cancelled subscriptions and non-matching filters are skipped")

	handled, _ = b.onPublish(paho.PublishReceived{Packet: &paho.Publish{Topic: "nobody/home"}})
	assert.False(t, handled)
}

func TestBroker_ClosedGuards(t *testing.T) {
	b := &Broker{subs: map[*subscription]struct{}{}}
	b.closed.Store(true)
	assert.Error(t, b.Publish(context.Background(), ports.Message{Topic: "x"}))
	_, err := b.Subscribe(context.Background(), "x/#", func(context.Context, ports.Message) {})
	assert.Error(t, err)
	_, err = b.Subscribe(context.Background(), "bad/#/x", func(context.Context, ports.Message) {})
	assert.Error(t, err)
	assert.False(t, b.Ready())
	assert.NoError(t, b.Close(), "double close is a no-op")
}
