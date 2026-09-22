// Package uns defines the Unified Namespace contract: the ISA-95 topic
// hierarchy and the Sparkplug-B-inspired payload every publisher and consumer
// in plantstream agrees on. It is pure logic with no I/O.
package uns

import (
	"errors"
	"fmt"
	"strings"
)

// Topic is an ISA-95 equipment-hierarchy address:
//
//	enterprise/site/area/line/cell/tag
//
// Every segment is a plain identifier; wildcards and slashes are rejected so
// a topic is always a concrete publish address.
type Topic struct {
	Enterprise string
	Site       string
	Area       string
	Line       string
	Cell       string
	Tag        string
}

// EdgeSegment is the reserved segment under <enterprise>/<site> that carries
// edge-node lifecycle messages (BIRTH/DEATH). It uses a leading underscore
// rather than "$" because "$"-prefixed topics are reserved by MQTT brokers.
const EdgeSegment = "_edge"

// ErrInvalidSegment is returned when a topic segment is not a valid identifier.
var ErrInvalidSegment = errors.New("uns: invalid topic segment")

// ValidateSegment checks a single topic segment.
func ValidateSegment(s string) error {
	switch {
	case s == "":
		return fmt.Errorf("%w: empty", ErrInvalidSegment)
	case strings.ContainsAny(s, "/+# \t\r\n"):
		return fmt.Errorf("%w: %q contains a reserved character", ErrInvalidSegment, s)
	case strings.HasPrefix(s, "$"):
		return fmt.Errorf("%w: %q starts with '$' (reserved by MQTT)", ErrInvalidSegment, s)
	case len(s) > 128:
		return fmt.Errorf("%w: %q is longer than 128 bytes", ErrInvalidSegment, s)
	}
	return nil
}

// Validate checks all six segments.
func (t Topic) Validate() error {
	for _, seg := range []struct{ name, v string }{
		{"enterprise", t.Enterprise}, {"site", t.Site}, {"area", t.Area},
		{"line", t.Line}, {"cell", t.Cell}, {"tag", t.Tag},
	} {
		if err := ValidateSegment(seg.v); err != nil {
			return fmt.Errorf("%s: %w", seg.name, err)
		}
	}
	return nil
}

// String renders the topic path.
func (t Topic) String() string {
	return strings.Join([]string{t.Enterprise, t.Site, t.Area, t.Line, t.Cell, t.Tag}, "/")
}

// ParseTopic parses a six-segment data topic.
func ParseTopic(s string) (Topic, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 6 {
		return Topic{}, fmt.Errorf("uns: topic %q must have 6 segments, has %d", s, len(parts))
	}
	t := Topic{parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]}
	if err := t.Validate(); err != nil {
		return Topic{}, err
	}
	return t, nil
}

// LifecycleTopic returns the topic used for BIRTH/DEATH of an edge node:
//
//	enterprise/site/_edge/<node>/<BIRTH|DEATH>
func LifecycleTopic(enterprise, site, node string, mt MessageType) string {
	return strings.Join([]string{enterprise, site, EdgeSegment, node, string(mt)}, "/")
}

// MatchFilter reports whether topic matches an MQTT topic filter using the
// standard single-level (+) and multi-level (#) wildcards.
func MatchFilter(filter, topic string) bool {
	if filter == "#" {
		return true
	}
	fp := strings.Split(filter, "/")
	tp := strings.Split(topic, "/")
	for i, f := range fp {
		if f == "#" {
			return i == len(fp)-1
		}
		if i >= len(tp) {
			return false
		}
		if f != "+" && f != tp[i] {
			return false
		}
	}
	return len(fp) == len(tp)
}

// ValidateFilter checks that a subscription filter is well-formed.
func ValidateFilter(filter string) error {
	if filter == "" {
		return errors.New("uns: empty filter")
	}
	parts := strings.Split(filter, "/")
	for i, p := range parts {
		switch {
		case p == "#":
			if i != len(parts)-1 {
				return fmt.Errorf("uns: '#' must be the last segment in %q", filter)
			}
		case p == "+":
		case strings.ContainsAny(p, "+#"):
			return fmt.Errorf("uns: wildcard must occupy a whole segment in %q", filter)
		case p == "":
			return fmt.Errorf("uns: empty segment in filter %q", filter)
		}
	}
	return nil
}
