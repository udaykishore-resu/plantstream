// Package edge is the edge node pipeline: it drives collectors, stamps every
// reading with asset context, runs the quality rules, publishes Sparkplug-style
// payloads to the UNS and keeps the last value of every tag for the API.
package edge

import (
	"sort"
	"sync"

	"github.com/udaykishore-resu/plantstream/internal/domain/uns"
)

// Latest is a thread-safe last-value cache keyed by asset and topic.
type Latest struct {
	mu      sync.RWMutex
	byAsset map[string]map[string]uns.Metric
	byTopic map[string]uns.Metric
	counts  map[uns.Quality]int
}

// NewLatest creates an empty cache.
func NewLatest() *Latest {
	return &Latest{byAsset: map[string]map[string]uns.Metric{}, byTopic: map[string]uns.Metric{}, counts: map[uns.Quality]int{}}
}

// Set stores m for assetID and returns the previous quality (if any).
func (l *Latest) Set(assetID string, m uns.Metric) (prev uns.Quality, had bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	tags, ok := l.byAsset[assetID]
	if !ok {
		tags = map[string]uns.Metric{}
		l.byAsset[assetID] = tags
	}
	if old, ok := tags[m.Name]; ok {
		prev, had = old.Quality, true
		l.counts[old.Quality]--
	}
	tags[m.Name] = m
	l.byTopic[m.Topic] = m
	l.counts[m.Quality]++
	return prev, had
}

// Asset returns the metrics of one asset sorted by tag name.
func (l *Latest) Asset(assetID string) ([]uns.Metric, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	tags, ok := l.byAsset[assetID]
	if !ok {
		return nil, false
	}
	out := make([]uns.Metric, 0, len(tags))
	for _, m := range tags {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, true
}

// Topic returns the last metric published on a topic.
func (l *Latest) Topic(topic string) (uns.Metric, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	m, ok := l.byTopic[topic]
	return m, ok
}

// Counts returns the number of tags per quality code.
func (l *Latest) Counts() map[uns.Quality]int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make(map[uns.Quality]int, len(l.counts))
	for k, v := range l.counts {
		out[k] = v
	}
	return out
}
