// Package config loads the 12-factor environment configuration of the
// plantstream edge node, applies defaults and validates it.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully resolved runtime configuration.
type Config struct {
	HTTPAddr        string
	PlantFile       string
	LogLevel        string
	ShutdownTimeout time.Duration
	StaleCheck      time.Duration

	Broker         string // memory | mqtt
	MQTTURL        string
	MQTTClientID   string
	MQTTUsername   string
	MQTTPassword   string
	MQTTQoS        byte
	MQTTKeepAlive  uint16
	MQTTConnectTmo time.Duration

	BridgeEnabled     bool
	BridgeFilter      string
	BridgePerTopicMax int
	BridgeInboxSize   int
	Sink              string // memory | kafka
	KafkaBrokers      []string
	KafkaTopic        string
	QueueDir          string // empty → in-memory queue
	QueueMaxBytes     int64
	QueueSync         bool

	OTelEndpoint     string
	OTelInsecure     bool
	TraceSampleRatio float64
	ServiceVersion   string
}

// Lookup mirrors os.LookupEnv so tests can inject an environment.
type Lookup func(string) (string, bool)

// Prefix of every plantstream variable.
const Prefix = "PLANTSTREAM_"

// Load reads configuration from the process environment.
func Load() (Config, error) { return LoadFrom(os.LookupEnv) }

// LoadFrom reads configuration through lookup.
func LoadFrom(lookup Lookup) (Config, error) {
	e := env{lookup: lookup}
	c := Config{
		HTTPAddr:          e.str("HTTP_ADDR", ":8080"),
		PlantFile:         e.str("PLANT_FILE", "examples/plant.yaml"),
		LogLevel:          e.str("LOG_LEVEL", "info"),
		ShutdownTimeout:   e.dur("SHUTDOWN_TIMEOUT", 10*time.Second),
		StaleCheck:        e.dur("STALE_CHECK_INTERVAL", time.Second),
		Broker:            strings.ToLower(e.str("BROKER", "memory")),
		MQTTURL:           e.str("MQTT_URL", "mqtt://localhost:1883"),
		MQTTClientID:      e.str("MQTT_CLIENT_ID", ""),
		MQTTUsername:      e.str("MQTT_USERNAME", ""),
		MQTTPassword:      e.str("MQTT_PASSWORD", ""),
		MQTTQoS:           byte(e.int("MQTT_QOS", 1)),
		MQTTKeepAlive:     uint16(e.int("MQTT_KEEPALIVE_SECONDS", 30)),
		MQTTConnectTmo:    e.dur("MQTT_CONNECT_TIMEOUT", 10*time.Second),
		BridgeEnabled:     e.boolean("BRIDGE_ENABLED", false),
		BridgeFilter:      e.str("BRIDGE_FILTER", "#"),
		BridgePerTopicMax: e.int("BRIDGE_PER_TOPIC_MAX", 64),
		BridgeInboxSize:   e.int("BRIDGE_INBOX_SIZE", 4096),
		Sink:              strings.ToLower(e.str("SINK", "memory")),
		KafkaBrokers:      e.list("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:        e.str("KAFKA_TOPIC", "uns.events"),
		QueueDir:          e.str("QUEUE_DIR", ""),
		QueueMaxBytes:     e.int64("QUEUE_MAX_BYTES", 64<<20),
		QueueSync:         e.boolean("QUEUE_SYNC", false),
		OTelEndpoint:      e.str("OTEL_ENDPOINT", ""),
		OTelInsecure:      e.boolean("OTEL_INSECURE", true),
		TraceSampleRatio:  e.float("TRACE_SAMPLE_RATIO", 1),
		ServiceVersion:    e.str("VERSION", "dev"),
	}
	if c.OTelEndpoint == "" {
		if v, ok := lookup("OTEL_EXPORTER_OTLP_ENDPOINT"); ok {
			c.OTelEndpoint = strings.TrimPrefix(strings.TrimPrefix(v, "http://"), "https://")
		}
	}
	if e.err != nil {
		return Config{}, e.err
	}
	return c, c.Validate()
}

// Validate checks cross-field constraints.
func (c Config) Validate() error {
	var errs []error
	if c.HTTPAddr == "" {
		errs = append(errs, errors.New("HTTP_ADDR must not be empty"))
	}
	if c.PlantFile == "" {
		errs = append(errs, errors.New("PLANT_FILE must not be empty"))
	}
	switch c.Broker {
	case "memory", "mqtt":
	default:
		errs = append(errs, fmt.Errorf("BROKER must be memory|mqtt, got %q", c.Broker))
	}
	if c.Broker == "mqtt" && c.MQTTURL == "" {
		errs = append(errs, errors.New("MQTT_URL is required when BROKER=mqtt"))
	}
	if c.MQTTQoS > 1 {
		errs = append(errs, errors.New("MQTT_QOS must be 0 or 1"))
	}
	switch c.Sink {
	case "memory", "kafka":
	default:
		errs = append(errs, fmt.Errorf("SINK must be memory|kafka, got %q", c.Sink))
	}
	if c.BridgeEnabled && c.Sink == "kafka" && len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required when SINK=kafka"))
	}
	if c.BridgeEnabled && c.Sink == "kafka" && c.KafkaTopic == "" {
		errs = append(errs, errors.New("KAFKA_TOPIC is required when SINK=kafka"))
	}
	if c.QueueMaxBytes <= 0 {
		errs = append(errs, errors.New("QUEUE_MAX_BYTES must be > 0"))
	}
	if c.BridgePerTopicMax <= 0 || c.BridgeInboxSize <= 0 {
		errs = append(errs, errors.New("BRIDGE_PER_TOPIC_MAX and BRIDGE_INBOX_SIZE must be > 0"))
	}
	if c.ShutdownTimeout <= 0 || c.StaleCheck <= 0 {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT and STALE_CHECK_INTERVAL must be > 0"))
	}
	if c.TraceSampleRatio < 0 || c.TraceSampleRatio > 1 {
		errs = append(errs, errors.New("TRACE_SAMPLE_RATIO must be within [0,1]"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("config: %w", errors.Join(errs...))
	}
	return nil
}

type env struct {
	lookup Lookup
	err    error
}

func (e *env) raw(key string) (string, bool) {
	v, ok := e.lookup(Prefix + key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	return v, v != ""
}

func (e *env) fail(key string, err error) {
	e.err = errors.Join(e.err, fmt.Errorf("config: %s%s: %w", Prefix, key, err))
}

func (e *env) str(key, def string) string {
	if v, ok := e.raw(key); ok {
		return v
	}
	return def
}

func (e *env) int(key string, def int) int {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.fail(key, err)
		return def
	}
	return n
}

func (e *env) int64(key string, def int64) int64 {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		e.fail(key, err)
		return def
	}
	return n
}

func (e *env) float(key string, def float64) float64 {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		e.fail(key, err)
		return def
	}
	return f
}

func (e *env) dur(key string, def time.Duration) time.Duration {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.fail(key, err)
		return def
	}
	return d
}

func (e *env) boolean(key string, def bool) bool {
	v, ok := e.raw(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.fail(key, err)
		return def
	}
	return b
}

func (e *env) list(key, def string) []string {
	v := e.str(key, def)
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
