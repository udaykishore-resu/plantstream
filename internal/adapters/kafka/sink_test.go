package kafka

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// Full produce/consume behaviour is exercised against a real broker via
// docker-compose (make run-full); these tests cover config validation and the
// unreachable-cluster path the bridge depends on.

func TestNew_Validation(t *testing.T) {
	_, err := New(context.Background(), Config{}, nil)
	assert.Error(t, err)
	_, err = New(context.Background(), Config{Brokers: []string{"localhost:9"}}, nil)
	assert.Error(t, err)
}

func TestSink_UnreachableClusterIsNotFatal(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	s, err := New(ctx, Config{Brokers: []string{addr}, Topic: "uns", DeliveryTimeout: time.Second}, nil)
	require.NoError(t, err, "startup must not depend on Kafka being up")
	assert.False(t, s.Ready())

	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	err = s.Send(sctx, ports.Message{Topic: "acme/a/b/c/d/e", Payload: []byte("{}"), At: time.Now()})
	assert.Error(t, err, "send fails so the bridge stores and forwards")
	assert.False(t, s.Ready())

	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
	assert.Error(t, s.Send(context.Background(), ports.Message{Topic: "x"}))
}
