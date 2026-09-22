package bridge

import (
	"sync"

	"github.com/udaykishore-resu/plantstream/internal/ports"
)

// inbox is the in-memory stage between the broker callback and the
// forwarder. Every topic has its own bounded FIFO and the forwarder dequeues
// topics round-robin, so a chatty tag can neither reorder nor starve the
// others. When a topic's FIFO is full its oldest message is dropped
// (latest-value-wins coalescing) and counted; when the global budget is hit
// the oldest message of the longest FIFO is dropped.
type inbox struct {
	mu       sync.Mutex
	topics   map[string][]ports.Message
	ring     []string // topics with pending messages, round-robin order
	rr       int
	total    int
	perTopic int
	maxTotal int
	dropped  int64
	notify   chan struct{}
}

func newInbox(perTopic, maxTotal int) *inbox {
	return &inbox{topics: map[string][]ports.Message{}, perTopic: perTopic, maxTotal: maxTotal, notify: make(chan struct{}, 1)}
}

// push enqueues m and reports how many messages were dropped to make room.
func (in *inbox) push(m ports.Message) (dropped int) {
	in.mu.Lock()
	q := in.topics[m.Topic]
	if len(q) == 0 {
		in.ring = append(in.ring, m.Topic)
	}
	if len(q) >= in.perTopic {
		q = q[1:]
		in.total--
		dropped++
	} else if in.total >= in.maxTotal {
		if in.evictLongest() {
			dropped++
		}
	}
	in.topics[m.Topic] = append(q, m)
	in.total++
	in.dropped += int64(dropped)
	in.mu.Unlock()
	select {
	case in.notify <- struct{}{}:
	default:
	}
	return dropped
}

// evictLongest drops the oldest message of the longest FIFO (caller holds mu).
func (in *inbox) evictLongest() bool {
	var victim string
	longest := 0
	for _, t := range in.ring {
		if l := len(in.topics[t]); l > longest {
			victim, longest = t, l
		}
	}
	if longest == 0 {
		return false
	}
	q := in.topics[victim][1:]
	in.topics[victim] = q
	in.total--
	if len(q) == 0 {
		in.removeFromRing(victim)
	}
	return true
}

func (in *inbox) removeFromRing(topic string) {
	for i, t := range in.ring {
		if t == topic {
			in.ring = append(in.ring[:i], in.ring[i+1:]...)
			if i < in.rr && in.rr > 0 {
				in.rr--
			}
			break
		}
	}
	if len(in.ring) == 0 {
		in.rr = 0
	} else {
		in.rr %= len(in.ring)
	}
}

// pop dequeues the next message round-robin across topics.
func (in *inbox) pop() (ports.Message, bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.total == 0 || len(in.ring) == 0 {
		return ports.Message{}, false
	}
	in.rr %= len(in.ring)
	topic := in.ring[in.rr]
	q := in.topics[topic]
	m := q[0]
	q = q[1:]
	in.total--
	if len(q) == 0 {
		delete(in.topics, topic)
		in.ring = append(in.ring[:in.rr], in.ring[in.rr+1:]...)
		if len(in.ring) == 0 {
			in.rr = 0
		} else {
			in.rr %= len(in.ring)
		}
	} else {
		in.topics[topic] = q
		in.rr = (in.rr + 1) % len(in.ring)
	}
	return m, true
}

func (in *inbox) len() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.total
}

func (in *inbox) droppedCount() int64 {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.dropped
}
