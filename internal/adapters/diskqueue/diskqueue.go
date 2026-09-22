// Package diskqueue is a bounded, segmented, append-only on-disk FIFO used by
// the edge→cloud bridge for store-and-forward. Records are CRC-protected;
// the queue survives process restarts and enforces a byte budget by dropping
// the oldest segment (drop-oldest policy, see docs/adr/0004).
//
// Layout in dir:
//
//	seg-00000000000000000001.log   records, oldest segment first
//	seg-00000000000000000002.log
//	head                            "<segment id> <byte offset>\n" of the read cursor
//
// Record: uint32 body length | uint32 CRC32(IEEE) of body | body
// Body:   uint16 topic length | topic | int64 unix nanos | uint8 retain | payload
package diskqueue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/udaykishore-resu/plantstream/internal/ports"
)

const (
	segPrefix     = "seg-"
	segSuffix     = ".log"
	headFile      = "head"
	recHeaderLen  = 8
	maxRecordBody = 16 << 20 // 16 MiB: sanity bound when scanning

	// DefaultMaxBytes bounds the whole queue (64 MiB).
	DefaultMaxBytes int64 = 64 << 20
	// DefaultSegmentBytes is the roll-over size of one segment (4 MiB).
	DefaultSegmentBytes int64 = 4 << 20
)

// ErrCorrupt is returned when a record fails its CRC.
var ErrCorrupt = errors.New("diskqueue: corrupt record")

// Options configures Open.
type Options struct {
	MaxBytes     int64
	SegmentBytes int64
	// Sync forces fsync after every Push (safer, ~10× slower).
	Sync bool
}

type segment struct {
	id      uint64
	path    string
	size    int64
	records int
}

// Queue implements ports.Queue on disk.
type Queue struct {
	mu   sync.Mutex
	dir  string
	opts Options

	segments []*segment // oldest first; last is the write tail
	writer   *os.File   // tail, O_APPEND
	reader   *os.File   // head
	headOff  int64
	count    int
	bytes    int64
	dropped  int64
	peeked   *ports.Message
	peekLen  int64
	closed   bool
}

// Open creates or recovers a queue in dir.
func Open(dir string, opts Options) (*Queue, error) {
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = DefaultMaxBytes
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = DefaultSegmentBytes
	}
	if opts.SegmentBytes > opts.MaxBytes/2 {
		opts.SegmentBytes = max(opts.MaxBytes/2, 1)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("diskqueue: mkdir %s: %w", dir, err)
	}
	q := &Queue{dir: dir, opts: opts}
	if err := q.recover(); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *Queue) recover() error {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return fmt.Errorf("diskqueue: read dir: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, segPrefix) || !strings.HasSuffix(name, segSuffix) {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, segPrefix), segSuffix), 10, 64)
		if err != nil {
			continue
		}
		q.segments = append(q.segments, &segment{id: id, path: filepath.Join(q.dir, name)})
	}
	sort.Slice(q.segments, func(i, j int) bool { return q.segments[i].id < q.segments[j].id })

	headID, headOff := q.readHead()
	for i, s := range q.segments {
		start := int64(0)
		if s.id == headID {
			start = headOff
		}
		if s.id < headID {
			// Fully consumed segment left behind by a crash: drop it.
			_ = os.Remove(s.path)
			q.segments[i] = nil
			continue
		}
		size, records, err := scanSegment(s.path, start)
		if err != nil {
			return err
		}
		s.size, s.records = size, records
		if s.id == headID {
			q.headOff = min(headOff, size)
		}
		q.count += records
		q.bytes += size
	}
	live := q.segments[:0]
	for _, s := range q.segments {
		if s != nil {
			live = append(live, s)
		}
	}
	q.segments = live
	if len(q.segments) == 0 {
		if err := q.roll(1); err != nil {
			return err
		}
	} else if err := q.openWriter(); err != nil {
		return err
	}
	return q.enforceBound()
}

// scanSegment validates records from `from` on and truncates a torn tail.
func scanSegment(path string, from int64) (size int64, records int, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o640)
	if err != nil {
		return 0, 0, fmt.Errorf("diskqueue: open %s: %w", path, err)
	}
	defer f.Close()
	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return 0, 0, err
	}
	off := from
	hdr := make([]byte, recHeaderLen)
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			break // EOF or torn header
		}
		n := binary.BigEndian.Uint32(hdr[0:4])
		want := binary.BigEndian.Uint32(hdr[4:8])
		if n == 0 || n > maxRecordBody {
			break
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(f, body); err != nil {
			break
		}
		if crc32.ChecksumIEEE(body) != want {
			break
		}
		off += recHeaderLen + int64(n)
		records++
	}
	// Truncate anything after the last valid record (torn write on crash).
	if err := f.Truncate(off); err != nil {
		return 0, 0, fmt.Errorf("diskqueue: truncate %s: %w", path, err)
	}
	return off, records, nil
}

func (q *Queue) readHead() (uint64, int64) {
	b, err := os.ReadFile(filepath.Join(q.dir, headFile))
	if err != nil {
		return 0, 0
	}
	fields := strings.Fields(string(b))
	if len(fields) != 2 {
		return 0, 0
	}
	id, err1 := strconv.ParseUint(fields[0], 10, 64)
	off, err2 := strconv.ParseInt(fields[1], 10, 64)
	if err1 != nil || err2 != nil || off < 0 {
		return 0, 0
	}
	return id, off
}

func (q *Queue) writeHead() error {
	if len(q.segments) == 0 {
		return nil
	}
	tmp := filepath.Join(q.dir, headFile+".tmp")
	data := strconv.FormatUint(q.segments[0].id, 10) + " " + strconv.FormatInt(q.headOff, 10) + "\n"
	if err := os.WriteFile(tmp, []byte(data), 0o640); err != nil {
		return fmt.Errorf("diskqueue: write head: %w", err)
	}
	return os.Rename(tmp, filepath.Join(q.dir, headFile))
}

func (q *Queue) openWriter() error {
	if q.writer != nil {
		_ = q.writer.Close()
	}
	tail := q.segments[len(q.segments)-1]
	f, err := os.OpenFile(tail.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("diskqueue: open tail: %w", err)
	}
	q.writer = f
	return nil
}

func (q *Queue) roll(id uint64) error {
	s := &segment{id: id, path: filepath.Join(q.dir, fmt.Sprintf("%s%020d%s", segPrefix, id, segSuffix))}
	q.segments = append(q.segments, s)
	return q.openWriter()
}

// Push appends msg.
func (q *Queue) Push(msg ports.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return errors.New("diskqueue: closed")
	}
	rec := encode(msg)
	tail := q.segments[len(q.segments)-1]
	if tail.size > 0 && tail.size+int64(len(rec)) > q.opts.SegmentBytes {
		if err := q.roll(tail.id + 1); err != nil {
			return err
		}
		tail = q.segments[len(q.segments)-1]
	}
	if _, err := q.writer.Write(rec); err != nil {
		return fmt.Errorf("diskqueue: append: %w", err)
	}
	if q.opts.Sync {
		if err := q.writer.Sync(); err != nil {
			return fmt.Errorf("diskqueue: fsync: %w", err)
		}
	}
	tail.size += int64(len(rec))
	tail.records++
	q.count++
	q.bytes += int64(len(rec))
	return q.enforceBound()
}

// enforceBound drops whole oldest segments while over budget. The tail is
// never dropped so the most recent data always survives.
func (q *Queue) enforceBound() error {
	for q.bytes > q.opts.MaxBytes && len(q.segments) > 1 {
		if err := q.dropHead(true); err != nil {
			return err
		}
	}
	return nil
}

// dropHead removes the oldest segment. When counting, its unread records are
// added to the dropped counter.
func (q *Queue) dropHead(counting bool) error {
	head := q.segments[0]
	if counting {
		q.dropped += int64(head.records)
	}
	q.count -= head.records
	q.bytes -= head.size
	if q.reader != nil {
		_ = q.reader.Close()
		q.reader = nil
	}
	q.peeked = nil
	q.headOff = 0
	if err := os.Remove(head.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("diskqueue: remove segment: %w", err)
	}
	q.segments = q.segments[1:]
	return q.writeHead()
}

// Peek returns the oldest unread message.
func (q *Queue) Peek() (ports.Message, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.peekLocked()
}

func (q *Queue) peekLocked() (ports.Message, bool, error) {
	if q.count == 0 {
		return ports.Message{}, false, nil
	}
	if q.peeked != nil {
		return *q.peeked, true, nil
	}
	head := q.segments[0]
	if q.reader == nil {
		f, err := os.Open(head.path)
		if err != nil {
			return ports.Message{}, false, fmt.Errorf("diskqueue: open head: %w", err)
		}
		q.reader = f
	}
	hdr := make([]byte, recHeaderLen)
	if _, err := q.reader.ReadAt(hdr, q.headOff); err != nil {
		return ports.Message{}, false, fmt.Errorf("diskqueue: read header: %w", err)
	}
	n := binary.BigEndian.Uint32(hdr[0:4])
	want := binary.BigEndian.Uint32(hdr[4:8])
	if n == 0 || n > maxRecordBody {
		return ports.Message{}, false, ErrCorrupt
	}
	body := make([]byte, n)
	if _, err := q.reader.ReadAt(body, q.headOff+recHeaderLen); err != nil {
		return ports.Message{}, false, fmt.Errorf("diskqueue: read body: %w", err)
	}
	if crc32.ChecksumIEEE(body) != want {
		return ports.Message{}, false, ErrCorrupt
	}
	msg, err := decode(body)
	if err != nil {
		return ports.Message{}, false, err
	}
	q.peeked = &msg
	q.peekLen = recHeaderLen + int64(n)
	return msg, true, nil
}

// Ack removes the message last returned by Peek.
func (q *Queue) Ack() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == 0 {
		return nil
	}
	if q.peeked == nil {
		// Ack without a prior Peek: locate the record to learn its length.
		_, ok, err := q.peekLocked()
		if err != nil {
			// The head record is unreadable, so nothing behind it in this
			// segment can be trusted either: discard the segment.
			if len(q.segments) > 1 {
				return q.dropHead(true)
			}
			return q.resetOnly(true)
		}
		if !ok {
			return nil
		}
	}
	head := q.segments[0]
	q.headOff += q.peekLen
	q.count--
	head.records--
	q.peeked = nil

	if q.headOff >= head.size {
		if len(q.segments) > 1 {
			return q.dropHead(false)
		}
		// Only segment and fully consumed: reclaim its space.
		return q.resetOnly(false)
	}
	return q.writeHead()
}

// resetOnly truncates the sole remaining segment back to empty.
func (q *Queue) resetOnly(counting bool) error {
	head := q.segments[0]
	if counting {
		q.dropped += int64(head.records)
	}
	if q.reader != nil {
		_ = q.reader.Close()
		q.reader = nil
	}
	q.peeked = nil
	if err := os.Truncate(head.path, 0); err != nil {
		return fmt.Errorf("diskqueue: reset segment: %w", err)
	}
	head.size, head.records, q.headOff, q.bytes, q.count = 0, 0, 0, 0, 0
	return q.writeHead()
}

// Len returns the number of unread messages.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

// Bytes returns the bytes currently occupied on disk (the bounded quantity).
func (q *Queue) Bytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes
}

// Dropped returns how many records were evicted by the byte bound.
func (q *Queue) Dropped() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}

// Close flushes the head cursor and closes files.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	var errs []error
	if err := q.writeHead(); err != nil {
		errs = append(errs, err)
	}
	if q.writer != nil {
		errs = append(errs, q.writer.Close())
	}
	if q.reader != nil {
		errs = append(errs, q.reader.Close())
	}
	return errors.Join(errs...)
}

func encode(m ports.Message) []byte {
	topic := []byte(m.Topic)
	if len(topic) > 0xFFFF {
		topic = topic[:0xFFFF]
	}
	bodyLen := 2 + len(topic) + 8 + 1 + len(m.Payload)
	buf := make([]byte, recHeaderLen+bodyLen)
	body := buf[recHeaderLen:]
	binary.BigEndian.PutUint16(body[0:2], uint16(len(topic)))
	copy(body[2:], topic)
	off := 2 + len(topic)
	binary.BigEndian.PutUint64(body[off:], uint64(m.At.UnixNano()))
	off += 8
	if m.Retain {
		body[off] = 1
	}
	off++
	copy(body[off:], m.Payload)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(body))
	return buf
}

func decode(body []byte) (ports.Message, error) {
	if len(body) < 2 {
		return ports.Message{}, ErrCorrupt
	}
	tl := int(binary.BigEndian.Uint16(body[0:2]))
	if len(body) < 2+tl+9 {
		return ports.Message{}, ErrCorrupt
	}
	topic := string(body[2 : 2+tl])
	off := 2 + tl
	nanos := int64(binary.BigEndian.Uint64(body[off:]))
	off += 8
	retain := body[off] == 1
	off++
	payload := make([]byte, len(body)-off)
	copy(payload, body[off:])
	return ports.Message{Topic: topic, Payload: payload, Retain: retain, At: time.Unix(0, nanos).UTC()}, nil
}
