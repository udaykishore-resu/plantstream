package uns

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopic_RoundtripAndValidation(t *testing.T) {
	tp := Topic{"acme", "austin", "packaging", "line-1", "filler-01", "speed"}
	require.NoError(t, tp.Validate())
	assert.Equal(t, "acme/austin/packaging/line-1/filler-01/speed", tp.String())

	got, err := ParseTopic(tp.String())
	require.NoError(t, err)
	assert.Equal(t, tp, got)

	bad := []string{
		"acme/austin/packaging/line-1/filler-01",       // 5 segments
		"acme/austin/packaging/line-1/filler-01/a/b",   // 7 segments
		"acme/austin/packaging/line-1/filler-01/",      // empty tag
		"acme/austin/packaging/line-1/filler-01/sp+ed", // wildcard
		"acme/austin/packaging/line-1/filler-01/#",     // wildcard
		"$acme/austin/packaging/line-1/filler-01/speed",
		"acme/aus tin/packaging/line-1/filler-01/speed",
	}
	for _, s := range bad {
		_, err := ParseTopic(s)
		assert.Error(t, err, s)
	}
	assert.ErrorIs(t, ValidateSegment(""), ErrInvalidSegment)
	assert.Error(t, ValidateSegment(string(make([]byte, 129))))
}

func TestLifecycleTopic(t *testing.T) {
	assert.Equal(t, "acme/austin/_edge/plc-1/BIRTH", LifecycleTopic("acme", "austin", "plc-1", Birth))
	assert.Equal(t, "acme/austin/_edge/plc-1/DEATH", LifecycleTopic("acme", "austin", "plc-1", Death))
}

func TestMatchFilter(t *testing.T) {
	tests := []struct {
		filter, topic string
		want          bool
	}{
		{"#", "a/b/c", true},
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b/d", false},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/b/c/d", false},
		{"a/#", "a/b/c/d", true},
		{"a/#", "a", true}, // MQTT-v5 4.7.1.2: '#' also matches the parent level
		{"a/b/#", "a/b", true},
		{"a/b/#", "a/c", false},
		{"a/+", "a/b", true},
		{"a/+", "a", false},
		{"+/+/+", "a/b/c", true},
		{"+/+/+", "a/b", false},
		{"acme/austin/+/+/+/speed", "acme/austin/packaging/line-1/filler-01/speed", true},
		{"acme/austin/+/+/+/speed", "acme/austin/packaging/line-1/filler-01/temp", false},
		{"acme/austin/#", "acme/austin/_edge/plc-1/BIRTH", true},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, MatchFilter(tc.filter, tc.topic), "%s ~ %s", tc.filter, tc.topic)
	}
}

func TestValidateFilter(t *testing.T) {
	assert.NoError(t, ValidateFilter("#"))
	assert.NoError(t, ValidateFilter("a/+/c/#"))
	assert.Error(t, ValidateFilter(""))
	assert.Error(t, ValidateFilter("a/#/c"))
	assert.Error(t, ValidateFilter("a/b+/c"))
	assert.Error(t, ValidateFilter("a//c"))
}

func TestPayload_Golden(t *testing.T) {
	ts := time.Date(2026, 3, 14, 15, 9, 26, 535_000_000, time.UTC)
	lo, hi := 0.0, 600.0
	p := Payload{
		Type:      Data,
		Timestamp: Millis(ts),
		Seq:       7,
		Node:      "plc-line-1",
		Metrics: []Metric{{
			Name:      "speed",
			Timestamp: Millis(ts),
			DataType:  TypeFloat,
			Value:     480.25,
			Quality:   QualityGood,
			Topic:     "acme/austin/packaging/line-1/filler-01/speed",
			Context: Context{
				AssetID: "filler-01", AssetClass: "Filler", Unit: "bpm",
				EngLow: &lo, EngHigh: &hi, Source: "plc-line-1",
			},
		}},
	}
	got, err := p.Marshal()
	require.NoError(t, err)

	golden := filepath.Join("testdata", "data_payload.golden.json")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		require.NoError(t, os.WriteFile(golden, got, 0o644))
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(got))

	back, err := Unmarshal(got)
	require.NoError(t, err)
	assert.Equal(t, p.Type, back.Type)
	assert.Equal(t, ts, back.Timestamp.Time())
	assert.Equal(t, uint8(7), back.Seq)
	require.Len(t, back.Metrics, 1)
	assert.Equal(t, 480.25, back.Metrics[0].Value)
	assert.Equal(t, "Filler", back.Metrics[0].Context.AssetClass)
	assert.Equal(t, hi, *back.Metrics[0].Context.EngHigh)
}

func TestUnmarshal_Errors(t *testing.T) {
	_, err := Unmarshal([]byte("{"))
	assert.Error(t, err)
	_, err = Unmarshal([]byte(`{"type":"NOPE","node":"x"}`))
	assert.Error(t, err)
	_, err = Unmarshal([]byte(`{"type":"DATA"}`))
	assert.Error(t, err)
	_, err = Unmarshal([]byte(`{"type":"DATA","node":"n","timestamp":"abc"}`))
	assert.Error(t, err)
}

func TestMillis_Null(t *testing.T) {
	b, err := json.Marshal(Millis(time.Time{}))
	require.NoError(t, err)
	assert.Equal(t, "null", string(b))
	var m Millis
	require.NoError(t, json.Unmarshal([]byte("null"), &m))
	assert.True(t, m.Time().IsZero())
}

func TestSequencerWraps(t *testing.T) {
	var s Sequencer
	for i := 0; i < 256; i++ {
		assert.Equal(t, uint8(i), s.Next())
	}
	assert.Equal(t, uint8(0), s.Next())
	s.Reset()
	assert.Equal(t, uint8(0), s.Next())
}

func TestQualitySeverity(t *testing.T) {
	assert.Less(t, QualityGood.Severity(), QualityStale.Severity())
	assert.Less(t, QualityStale.Severity(), QualityFlatline.Severity())
	assert.Less(t, QualityFlatline.Severity(), QualityOutOfRange.Severity())
	assert.Less(t, QualityOutOfRange.Severity(), QualityBad.Severity())
	assert.Less(t, QualityBad.Severity(), Quality("weird").Severity())
}

func TestParseDataType(t *testing.T) {
	dt, err := ParseDataType("")
	require.NoError(t, err)
	assert.Equal(t, TypeFloat, dt)
	_, err = ParseDataType("Int64")
	assert.Error(t, err)
}
