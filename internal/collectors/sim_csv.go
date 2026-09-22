package collectors

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/udaykishore-resu/plantstream/internal/domain/asset"
	"github.com/udaykishore-resu/plantstream/internal/domain/linesim"
)

// SimSource runs the line model in-process and exposes its signals as tags.
type SimSource struct {
	name string
	tags map[string]string // tag → signal

	mu   sync.Mutex
	line *linesim.Line
	last time.Time
	now  func() time.Time
}

// maxSimStep caps the model step so a paused process does not jump the state
// machine hours ahead in one tick.
const maxSimStep = 5 * time.Second

// NewSimSource builds a simulator source.
func NewSimSource(src *asset.Source, tags []asset.TagRef) (*SimSource, error) {
	seed := int64(1)
	if src.Sim != nil && src.Sim.Seed != 0 {
		seed = src.Sim.Seed
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if t.Tag.Sim == nil || !linesim.IsSignal(t.Tag.Sim.Signal) {
			return nil, fmt.Errorf("collectors: tag %q: invalid sim signal", t.Tag.Name)
		}
		m[t.Tag.Name] = t.Tag.Sim.Signal
	}
	return &SimSource{name: src.Name, tags: m, line: linesim.NewLine(seed), now: time.Now}, nil
}

// Name implements Source.
func (s *SimSource) Name() string { return s.name }

// Read implements Source: advances the model by the wall-clock time elapsed
// since the previous read and samples every bound signal.
func (s *SimSource) Read(_ context.Context) ([]Reading, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if !s.last.IsZero() {
		dt := now.Sub(s.last)
		if dt > maxSimStep {
			dt = maxSimStep
		}
		s.line.Step(dt)
	}
	s.last = now
	snap := s.line.Snapshot()
	out := make([]Reading, 0, len(s.tags))
	for tag, sig := range s.tags {
		v, _ := snap.Get(sig)
		out = append(out, Reading{Tag: tag, Value: v, At: now})
	}
	return out, nil
}

// Close implements Source.
func (s *SimSource) Close() error { return nil }

// CSVSource replays a wide CSV file, one row per poll cycle:
//
//	timestamp,speed,temperature
//	2026-01-01T00:00:00Z,480.2,71.5
//
// Column names are matched to tags via csv.column. The timestamp column is
// optional and ignored for sampling time (replays are stamped with now).
// At end of file the source loops when configured, otherwise it returns no
// readings so tags go STALE, which is the honest signal for "file exhausted".
type CSVSource struct {
	name    string
	path    string
	loop    bool
	columns map[string]string // tag → column

	mu     sync.Mutex
	file   *os.File
	reader *csv.Reader
	index  map[string]int // column → position
	done   bool
	now    func() time.Time
}

// NewCSVSource builds a CSV replay source. The file is opened lazily so a
// missing file surfaces as a poll failure (BAD quality) instead of a crash.
func NewCSVSource(src *asset.Source, tags []asset.TagRef) (*CSVSource, error) {
	if src.CSV == nil || src.CSV.Path == "" {
		return nil, fmt.Errorf("collectors: source %q: missing csv config", src.Name)
	}
	cols := make(map[string]string, len(tags))
	for _, t := range tags {
		col := t.Tag.Name
		if t.Tag.CSV != nil && t.Tag.CSV.Column != "" {
			col = t.Tag.CSV.Column
		}
		cols[t.Tag.Name] = col
	}
	return &CSVSource{name: src.Name, path: src.CSV.Path, loop: src.CSV.Loop, columns: cols, now: time.Now}, nil
}

// Name implements Source.
func (s *CSVSource) Name() string { return s.name }

func (s *CSVSource) open() error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	r := csv.NewReader(f)
	r.TrimLeadingSpace = true
	r.ReuseRecord = false
	header, err := r.Read()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("read header: %w", err)
	}
	idx := make(map[string]int, len(header))
	for i, h := range header {
		idx[strings.TrimSpace(h)] = i
	}
	for tag, col := range s.columns {
		if _, ok := idx[col]; !ok {
			_ = f.Close()
			return fmt.Errorf("column %q for tag %q not in header %v", col, tag, header)
		}
	}
	s.file, s.reader, s.index = f, r, idx
	return nil
}

// Read implements Source.
func (s *CSVSource) Read(_ context.Context) ([]Reading, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return nil, nil
	}
	if s.reader == nil {
		if err := s.open(); err != nil {
			return nil, fmt.Errorf("open %s: %w", s.path, err)
		}
	}
	row, err := s.reader.Read()
	if errors.Is(err, io.EOF) {
		_ = s.file.Close()
		s.reader, s.file = nil, nil
		if !s.loop {
			s.done = true
			return nil, nil
		}
		if err := s.open(); err != nil {
			return nil, fmt.Errorf("reopen %s: %w", s.path, err)
		}
		if row, err = s.reader.Read(); err != nil {
			return nil, fmt.Errorf("%s has a header but no rows", s.path)
		}
	} else if err != nil {
		return nil, fmt.Errorf("read row: %w", err)
	}
	now := s.now()
	out := make([]Reading, 0, len(s.columns))
	for tag, col := range s.columns {
		i := s.index[col]
		if i >= len(row) {
			return nil, fmt.Errorf("row has %d fields, column %q is #%d", len(row), col, i+1)
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(row[i]), 64)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col, err)
		}
		out = append(out, Reading{Tag: tag, Value: v, At: now})
	}
	return out, nil
}

// Close implements Source.
func (s *CSVSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		err := s.file.Close()
		s.file, s.reader = nil, nil
		return err
	}
	return nil
}
