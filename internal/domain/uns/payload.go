package uns

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// MessageType mirrors the Sparkplug-B lifecycle: BIRTH announces a node and
// the metrics it will publish, DATA carries samples, DEATH is the last will.
type MessageType string

// Message types.
const (
	Birth MessageType = "BIRTH"
	Data  MessageType = "DATA"
	Death MessageType = "DEATH"
)

// DataType is the logical type of a metric value (Sparkplug-B names).
type DataType string

// Supported metric data types.
const (
	TypeInt16   DataType = "Int16"
	TypeUInt16  DataType = "UInt16"
	TypeInt32   DataType = "Int32"
	TypeUInt32  DataType = "UInt32"
	TypeFloat   DataType = "Float"
	TypeDouble  DataType = "Double"
	TypeBoolean DataType = "Boolean"
	TypeString  DataType = "String"
)

// ParseDataType validates a data type string; empty defaults to Float.
func ParseDataType(s string) (DataType, error) {
	switch DataType(s) {
	case "":
		return TypeFloat, nil
	case TypeInt16, TypeUInt16, TypeInt32, TypeUInt32, TypeFloat, TypeDouble, TypeBoolean, TypeString:
		return DataType(s), nil
	default:
		return "", fmt.Errorf("uns: unsupported datatype %q", s)
	}
}

// Quality is the quality code attached to every metric. Codes are ordered by
// severity: a metric carries the worst applicable code.
type Quality string

// Quality codes.
const (
	QualityGood       Quality = "GOOD"
	QualityStale      Quality = "STALE"
	QualityFlatline   Quality = "FLATLINE"
	QualityOutOfRange Quality = "OUT_OF_RANGE"
	QualityBad        Quality = "BAD"
)

// Severity ranks quality codes; higher is worse.
func (q Quality) Severity() int {
	switch q {
	case QualityGood:
		return 0
	case QualityStale:
		return 1
	case QualityFlatline:
		return 2
	case QualityOutOfRange:
		return 3
	case QualityBad:
		return 4
	default:
		return 5
	}
}

// Millis is a time serialised as Unix epoch milliseconds (Sparkplug style).
type Millis time.Time

// MarshalJSON implements json.Marshaler.
func (m Millis) MarshalJSON() ([]byte, error) {
	t := time.Time(m)
	if t.IsZero() {
		return []byte("null"), nil
	}
	return strconv.AppendInt(nil, t.UnixMilli(), 10), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (m *Millis) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*m = Millis(time.Time{})
		return nil
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return fmt.Errorf("uns: timestamp must be epoch millis: %w", err)
	}
	*m = Millis(time.UnixMilli(n).UTC())
	return nil
}

// Time converts back to time.Time.
func (m Millis) Time() time.Time { return time.Time(m) }

// Context is the asset context stamped on every metric so a consumer never
// needs to look anything up to understand a value.
type Context struct {
	AssetID     string   `json:"asset_id"`
	AssetClass  string   `json:"asset_class"`
	Unit        string   `json:"unit,omitempty"`
	EngLow      *float64 `json:"eng_low,omitempty"`
	EngHigh     *float64 `json:"eng_high,omitempty"`
	Source      string   `json:"source"`
	Description string   `json:"description,omitempty"`
	// QualityRule is the versioned rule that produced a non-GOOD quality.
	QualityRule string `json:"quality_rule,omitempty"`
}

// Metric is one sample of one tag.
type Metric struct {
	Name      string   `json:"name"`
	Timestamp Millis   `json:"timestamp"`
	DataType  DataType `json:"datatype"`
	Value     any      `json:"value"`
	Quality   Quality  `json:"quality"`
	Topic     string   `json:"topic,omitempty"`
	Context   Context  `json:"properties"`
}

// Payload is the message body published on a topic.
type Payload struct {
	Type      MessageType `json:"type"`
	Timestamp Millis      `json:"timestamp"`
	// Seq is a per-node sequence number that wraps at 256, like Sparkplug-B,
	// so consumers can detect gaps and reordering.
	Seq     uint8    `json:"seq"`
	Node    string   `json:"node"`
	Metrics []Metric `json:"metrics"`
}

// Marshal encodes the payload as compact JSON.
func (p Payload) Marshal() ([]byte, error) {
	return json.Marshal(p)
}

// Unmarshal decodes a payload, validating the essentials.
func Unmarshal(b []byte) (Payload, error) {
	var p Payload
	if err := json.Unmarshal(b, &p); err != nil {
		return Payload{}, fmt.Errorf("uns: decode payload: %w", err)
	}
	switch p.Type {
	case Birth, Data, Death:
	default:
		return Payload{}, fmt.Errorf("uns: unknown message type %q", p.Type)
	}
	if p.Node == "" {
		return Payload{}, errors.New("uns: payload missing node")
	}
	return p, nil
}

// Sequencer hands out wrapping per-node sequence numbers. BIRTH resets to 0.
type Sequencer struct{ next uint8 }

// Reset restarts the sequence (call on BIRTH).
func (s *Sequencer) Reset() { s.next = 0 }

// Next returns the current sequence number and advances.
func (s *Sequencer) Next() uint8 {
	n := s.next
	s.next++
	return n
}
