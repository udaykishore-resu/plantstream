package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lookup(m map[string]string) Lookup {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadFrom_Defaults(t *testing.T) {
	c, err := LoadFrom(lookup(nil))
	require.NoError(t, err)
	assert.Equal(t, ":8080", c.HTTPAddr)
	assert.Equal(t, "examples/plant.yaml", c.PlantFile)
	assert.Equal(t, "memory", c.Broker)
	assert.Equal(t, "memory", c.Sink)
	assert.False(t, c.BridgeEnabled)
	assert.Equal(t, []string{"localhost:9092"}, c.KafkaBrokers)
	assert.Equal(t, int64(64<<20), c.QueueMaxBytes)
	assert.Equal(t, 10*time.Second, c.ShutdownTimeout)
	assert.Equal(t, 1.0, c.TraceSampleRatio)
}

func TestLoadFrom_Overrides(t *testing.T) {
	c, err := LoadFrom(lookup(map[string]string{
		"PLANTSTREAM_HTTP_ADDR":          ":18700",
		"PLANTSTREAM_BROKER":             "MQTT",
		"PLANTSTREAM_MQTT_URL":           "mqtt://broker:1883",
		"PLANTSTREAM_MQTT_QOS":           "0",
		"PLANTSTREAM_BRIDGE_ENABLED":     "true",
		"PLANTSTREAM_SINK":               "kafka",
		"PLANTSTREAM_KAFKA_BROKERS":      "k1:9092, k2:9092 ,",
		"PLANTSTREAM_QUEUE_DIR":          "/var/lib/plantstream/queue",
		"PLANTSTREAM_QUEUE_MAX_BYTES":    "1024",
		"PLANTSTREAM_SHUTDOWN_TIMEOUT":   "3s",
		"PLANTSTREAM_TRACE_SAMPLE_RATIO": "0.25",
		"OTEL_EXPORTER_OTLP_ENDPOINT":    "http://otel:4318",
		"PLANTSTREAM_QUEUE_SYNC":         "1",
	}))
	require.NoError(t, err)
	assert.Equal(t, ":18700", c.HTTPAddr)
	assert.Equal(t, "mqtt", c.Broker)
	assert.Equal(t, byte(0), c.MQTTQoS)
	assert.True(t, c.BridgeEnabled)
	assert.Equal(t, []string{"k1:9092", "k2:9092"}, c.KafkaBrokers)
	assert.Equal(t, int64(1024), c.QueueMaxBytes)
	assert.Equal(t, 3*time.Second, c.ShutdownTimeout)
	assert.Equal(t, 0.25, c.TraceSampleRatio)
	assert.Equal(t, "otel:4318", c.OTelEndpoint)
	assert.True(t, c.QueueSync)
}

func TestLoadFrom_ParseErrors(t *testing.T) {
	_, err := LoadFrom(lookup(map[string]string{"PLANTSTREAM_MQTT_QOS": "x"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PLANTSTREAM_MQTT_QOS")
	for _, kv := range []map[string]string{
		{"PLANTSTREAM_SHUTDOWN_TIMEOUT": "soon"},
		{"PLANTSTREAM_BRIDGE_ENABLED": "maybe"},
		{"PLANTSTREAM_QUEUE_MAX_BYTES": "big"},
		{"PLANTSTREAM_TRACE_SAMPLE_RATIO": "half"},
	} {
		_, err := LoadFrom(lookup(kv))
		assert.Error(t, err, "%v", kv)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"bad broker", map[string]string{"PLANTSTREAM_BROKER": "nats"}, "BROKER"},
		{"bad sink", map[string]string{"PLANTSTREAM_SINK": "s3"}, "SINK"},
		{"qos", map[string]string{"PLANTSTREAM_MQTT_QOS": "2"}, "MQTT_QOS"},
		{"kafka brokers", map[string]string{"PLANTSTREAM_BRIDGE_ENABLED": "true", "PLANTSTREAM_SINK": "kafka", "PLANTSTREAM_KAFKA_BROKERS": ","}, "KAFKA_BROKERS"},
		{"queue bytes", map[string]string{"PLANTSTREAM_QUEUE_MAX_BYTES": "0"}, "QUEUE_MAX_BYTES"},
		{"inbox", map[string]string{"PLANTSTREAM_BRIDGE_INBOX_SIZE": "0"}, "BRIDGE_INBOX_SIZE"},
		{"stale", map[string]string{"PLANTSTREAM_STALE_CHECK_INTERVAL": "0s"}, "STALE_CHECK_INTERVAL"},
		{"ratio", map[string]string{"PLANTSTREAM_TRACE_SAMPLE_RATIO": "2"}, "TRACE_SAMPLE_RATIO"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadFrom(lookup(tc.env))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
	c := Config{}
	err := c.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_ADDR")
	assert.Contains(t, err.Error(), "PLANT_FILE")
}

func TestLoad_UsesProcessEnv(t *testing.T) {
	t.Setenv("PLANTSTREAM_HTTP_ADDR", ":18799")
	c, err := Load()
	require.NoError(t, err)
	assert.Equal(t, ":18799", c.HTTPAddr)
}
