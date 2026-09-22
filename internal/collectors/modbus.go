package collectors

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/udaykishore-resu/plantstream/internal/domain/asset"
	"github.com/udaykishore-resu/plantstream/internal/modbus"
)

// modbusTag is one tag's location inside a device.
type modbusTag struct {
	name  string
	fn    byte
	start uint16
	words uint16
	typ   modbus.DataType
	order modbus.ByteOrder
}

// Block is one coalesced read request covering several tags.
type Block struct {
	Function byte
	Start    uint16
	Count    uint16
	tags     []modbusTag
}

// Plan groups tags into the minimum number of FC 3/4 requests, bridging gaps
// of at most maxGap unused registers and never exceeding the protocol limit
// of 125 registers per request.
func Plan(tags []modbusTag, maxGap int) []Block {
	if len(tags) == 0 {
		return nil
	}
	sorted := make([]modbusTag, len(tags))
	copy(sorted, tags)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].fn != sorted[j].fn {
			return sorted[i].fn < sorted[j].fn
		}
		return sorted[i].start < sorted[j].start
	})
	var blocks []Block
	cur := Block{Function: sorted[0].fn, Start: sorted[0].start, Count: sorted[0].words, tags: []modbusTag{sorted[0]}}
	for _, t := range sorted[1:] {
		end := int(cur.Start) + int(cur.Count) // exclusive
		tEnd := int(t.start) + int(t.words)
		gap := int(t.start) - end
		newCount := max(tEnd, end) - int(cur.Start)
		if t.fn == cur.Function && gap <= maxGap && newCount <= int(modbus.MaxRegistersPerRead) {
			cur.Count = uint16(newCount)
			cur.tags = append(cur.tags, t)
			continue
		}
		blocks = append(blocks, cur)
		cur = Block{Function: t.fn, Start: t.start, Count: t.words, tags: []modbusTag{t}}
	}
	return append(blocks, cur)
}

// Tags returns the tag names covered by the block (for tests/diagnostics).
func (b Block) Tags() []string {
	out := make([]string, len(b.tags))
	for i, t := range b.tags {
		out[i] = t.name
	}
	return out
}

// dialer abstracts modbus.Dial for tests.
type dialer func(ctx context.Context, addr string, timeout time.Duration) (modbusConn, error)

type modbusConn interface {
	ReadRegisters(ctx context.Context, fn, unit byte, addr, qty uint16) ([]uint16, error)
	Close() error
}

func defaultDialer(ctx context.Context, addr string, timeout time.Duration) (modbusConn, error) {
	return modbus.Dial(ctx, addr, modbus.WithTimeout(timeout))
}

// ModbusSource polls a Modbus TCP device. It connects lazily and reconnects
// after any transport error; a protocol exception on one block is reported
// as a whole-cycle failure because it indicates a wrong address map.
type ModbusSource struct {
	name    string
	addr    string
	unit    byte
	timeout time.Duration
	blocks  []Block
	dial    dialer
	log     *slog.Logger

	mu   sync.Mutex
	conn modbusConn
	now  func() time.Time
}

// NewModbusSource builds a source from the plant definition.
func NewModbusSource(src *asset.Source, tags []asset.TagRef, log *slog.Logger) (*ModbusSource, error) {
	if src.Modbus == nil {
		return nil, fmt.Errorf("collectors: source %q: missing modbus config", src.Name)
	}
	if log == nil {
		log = slog.Default()
	}
	mtags := make([]modbusTag, 0, len(tags))
	for _, t := range tags {
		if t.Tag.Modbus == nil {
			return nil, fmt.Errorf("collectors: tag %q has no modbus address", t.Tag.Name)
		}
		typ, err := modbus.ParseDataType(string(t.Tag.Modbus.Type))
		if err != nil {
			return nil, fmt.Errorf("collectors: tag %q: %w", t.Tag.Name, err)
		}
		order, err := modbus.ParseByteOrder(string(t.Tag.Modbus.ByteOrder))
		if err != nil {
			return nil, fmt.Errorf("collectors: tag %q: %w", t.Tag.Name, err)
		}
		mtags = append(mtags, modbusTag{
			name: t.Tag.Name, fn: t.Tag.Modbus.FunctionCode(), start: t.Tag.Modbus.Register,
			words: typ.Words(), typ: typ, order: order,
		})
	}
	return &ModbusSource{
		name:    src.Name,
		addr:    src.Modbus.Address,
		unit:    src.Modbus.UnitID,
		timeout: src.Modbus.Timeout,
		blocks:  Plan(mtags, src.Modbus.MaxGap),
		dial:    defaultDialer,
		log:     log,
		now:     time.Now,
	}, nil
}

// Name implements Source.
func (s *ModbusSource) Name() string { return s.name }

// Blocks exposes the read plan.
func (s *ModbusSource) Blocks() []Block { return s.blocks }

// Read implements Source.
func (s *ModbusSource) Read(ctx context.Context) ([]Reading, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		c, err := s.dial(ctx, s.addr, s.timeout)
		if err != nil {
			return nil, fmt.Errorf("connect %s: %w", s.addr, err)
		}
		s.conn = c
		s.log.Info("modbus connected", "source", s.name, "addr", s.addr, "blocks", len(s.blocks))
	}
	at := s.now()
	out := make([]Reading, 0, 16)
	for _, b := range s.blocks {
		regs, err := s.conn.ReadRegisters(ctx, b.Function, s.unit, b.Start, b.Count)
		if err != nil {
			var ex *modbus.Exception
			if !errors.As(err, &ex) {
				// Transport-level problem: drop the connection so the next
				// cycle reconnects.
				_ = s.conn.Close()
				s.conn = nil
			}
			return nil, fmt.Errorf("read fc=%d start=%d count=%d: %w", b.Function, b.Start, b.Count, err)
		}
		for _, t := range b.tags {
			off := int(t.start - b.Start)
			v, err := modbus.Decode(t.typ, t.order, regs[off:off+int(t.words)])
			if err != nil {
				return nil, fmt.Errorf("decode %s: %w", t.name, err)
			}
			out = append(out, Reading{Tag: t.name, Value: v, At: at})
		}
	}
	return out, nil
}

// Close implements Source.
func (s *ModbusSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	return err
}
