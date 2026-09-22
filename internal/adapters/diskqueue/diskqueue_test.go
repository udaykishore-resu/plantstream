package diskqueue

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/udaykishore-resu/plantstream/internal/ports"
)

func msg(i int) ports.Message {
	return ports.Message{
		Topic:   fmt.Sprintf("acme/austin/pack/l1/c1/tag%d", i),
		Payload: []byte(fmt.Sprintf(`{"seq":%d,"pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`, i)),
		Retain:  i%2 == 0,
		At:      time.Unix(1_700_000_000+int64(i), 0).UTC(),
	}
}

func TestQueue_FIFO(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, Options{})
	require.NoError(t, err)
	defer q.Close()

	_, ok, err := q.Peek()
	require.NoError(t, err)
	assert.False(t, ok)
	require.NoError(t, q.Ack()) // empty ack is a no-op

	for i := 0; i < 10; i++ {
		require.NoError(t, q.Push(msg(i)))
	}
	assert.Equal(t, 10, q.Len())
	assert.Positive(t, q.Bytes())

	for i := 0; i < 10; i++ {
		m, ok, err := q.Peek()
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, msg(i), m, "message %d", i)
		// Peek is idempotent.
		m2, _, _ := q.Peek()
		assert.Equal(t, m, m2)
		require.NoError(t, q.Ack())
	}
	assert.Equal(t, 0, q.Len())
	assert.Equal(t, int64(0), q.Bytes(), "space reclaimed when drained")

	// Queue is reusable after draining.
	require.NoError(t, q.Push(msg(99)))
	m, ok, err := q.Peek()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, msg(99), m)
}

func TestQueue_AckWithoutPeek(t *testing.T) {
	q, err := Open(t.TempDir(), Options{})
	require.NoError(t, err)
	defer q.Close()
	require.NoError(t, q.Push(msg(1)))
	require.NoError(t, q.Push(msg(2)))
	require.NoError(t, q.Ack())
	m, ok, err := q.Peek()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, msg(2), m)
}

func TestQueue_SegmentsRollAndRecover(t *testing.T) {
	dir := t.TempDir()
	rec := len(encode(msg(0)))
	q, err := Open(dir, Options{MaxBytes: int64(rec * 100), SegmentBytes: int64(rec * 3)})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.NoError(t, q.Push(msg(i)))
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	assert.Equal(t, 4, len(segs), "10 records at 3 per segment")

	// Consume 4, crossing a segment boundary.
	for i := 0; i < 4; i++ {
		m, ok, err := q.Peek()
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, msg(i), m)
		require.NoError(t, q.Ack())
	}
	segs, _ = filepath.Glob(filepath.Join(dir, "seg-*.log"))
	assert.Equal(t, 3, len(segs), "consumed segment removed")
	require.NoError(t, q.Close())

	// Reopen: cursor and remaining records survive.
	q2, err := Open(dir, Options{MaxBytes: int64(rec * 100), SegmentBytes: int64(rec * 3)})
	require.NoError(t, err)
	defer q2.Close()
	assert.Equal(t, 6, q2.Len())
	for i := 4; i < 10; i++ {
		m, ok, err := q2.Peek()
		require.NoError(t, err)
		require.True(t, ok, "record %d", i)
		assert.Equal(t, msg(i), m)
		require.NoError(t, q2.Ack())
	}
	assert.Equal(t, 0, q2.Len())
}

func TestQueue_BoundDropsOldest(t *testing.T) {
	dir := t.TempDir()
	rec := int64(len(encode(msg(0))))
	q, err := Open(dir, Options{MaxBytes: rec * 6, SegmentBytes: rec * 2})
	require.NoError(t, err)
	defer q.Close()

	for i := 0; i < 20; i++ {
		require.NoError(t, q.Push(msg(i)))
	}
	assert.LessOrEqual(t, q.Bytes(), rec*6)
	assert.Positive(t, q.Dropped())
	assert.Equal(t, int(q.Dropped())+q.Len(), 20)

	// The newest record must always survive; the oldest survivor is contiguous.
	first, ok, err := q.Peek()
	require.NoError(t, err)
	require.True(t, ok)
	firstIdx := 20 - q.Len()
	assert.Equal(t, msg(firstIdx), first)
	for i := firstIdx; i < 20; i++ {
		m, ok, err := q.Peek()
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, msg(i), m)
		require.NoError(t, q.Ack())
	}
}

func TestQueue_TornTailTruncatedOnRecover(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, Options{})
	require.NoError(t, err)
	require.NoError(t, q.Push(msg(1)))
	require.NoError(t, q.Push(msg(2)))
	require.NoError(t, q.Close())

	segs, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	require.Len(t, segs, 1)
	f, err := os.OpenFile(segs[0], os.O_WRONLY|os.O_APPEND, 0o640)
	require.NoError(t, err)
	_, err = f.Write([]byte{0, 0, 0, 40, 1, 2, 3}) // torn header
	require.NoError(t, err)
	require.NoError(t, f.Close())

	q2, err := Open(dir, Options{})
	require.NoError(t, err)
	defer q2.Close()
	assert.Equal(t, 2, q2.Len())
	m, ok, err := q2.Peek()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, msg(1), m)

	// And appending after recovery still yields readable records.
	require.NoError(t, q2.Push(msg(3)))
	require.NoError(t, q2.Ack())
	require.NoError(t, q2.Ack())
	m, ok, err = q2.Peek()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, msg(3), m)
}

func TestQueue_CorruptBodyStopsScan(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, Options{})
	require.NoError(t, err)
	require.NoError(t, q.Push(msg(1)))
	require.NoError(t, q.Push(msg(2)))
	require.NoError(t, q.Close())

	segs, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	data, err := os.ReadFile(segs[0])
	require.NoError(t, err)
	// Flip a byte inside the second record's body.
	first := len(encode(msg(1)))
	data[first+recHeaderLen+3] ^= 0xFF
	require.NoError(t, os.WriteFile(segs[0], data, 0o640))

	q2, err := Open(dir, Options{})
	require.NoError(t, err)
	defer q2.Close()
	assert.Equal(t, 1, q2.Len(), "corrupt record and everything after it are discarded")
}

func TestQueue_BadHeadFileIgnored(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, Options{})
	require.NoError(t, err)
	require.NoError(t, q.Push(msg(1)))
	require.NoError(t, q.Close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, headFile), []byte("garbage"), 0o640))
	q2, err := Open(dir, Options{})
	require.NoError(t, err)
	defer q2.Close()
	assert.Equal(t, 1, q2.Len())
}

func TestQueue_StaleSegmentsBeforeHeadRemoved(t *testing.T) {
	dir := t.TempDir()
	rec := int64(len(encode(msg(0))))
	q, err := Open(dir, Options{MaxBytes: rec * 100, SegmentBytes: rec})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.NoError(t, q.Push(msg(i)))
	}
	require.NoError(t, q.Close())
	// Pretend a crash happened after the head advanced past segment 2 but
	// before the old files were removed.
	require.NoError(t, os.WriteFile(filepath.Join(dir, headFile), []byte("3 0\n"), 0o640))
	q2, err := Open(dir, Options{MaxBytes: rec * 100, SegmentBytes: rec})
	require.NoError(t, err)
	defer q2.Close()
	assert.Equal(t, 1, q2.Len())
	segs, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	assert.Len(t, segs, 1)
}

func TestQueue_ClosedAndSync(t *testing.T) {
	q, err := Open(t.TempDir(), Options{Sync: true})
	require.NoError(t, err)
	require.NoError(t, q.Push(msg(1)))
	require.NoError(t, q.Close())
	require.NoError(t, q.Close())
	assert.Error(t, q.Push(msg(2)))
}

func TestOpen_BadDir(t *testing.T) {
	f := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(f, []byte("x"), 0o640))
	_, err := Open(f, Options{})
	assert.Error(t, err)
}

func TestEncodeDecode(t *testing.T) {
	m := msg(5)
	body := encode(m)[recHeaderLen:]
	back, err := decode(body)
	require.NoError(t, err)
	assert.Equal(t, m, back)

	_, err = decode([]byte{0})
	assert.ErrorIs(t, err, ErrCorrupt)
	_, err = decode([]byte{0, 5, 'a'})
	assert.ErrorIs(t, err, ErrCorrupt)
}

func FuzzDecode(f *testing.F) {
	f.Add(encode(msg(1))[recHeaderLen:])
	f.Fuzz(func(t *testing.T, body []byte) {
		m, err := decode(body)
		if err != nil {
			return
		}
		again := encode(m)[recHeaderLen:]
		if len(m.Topic) <= 0xFFFF && string(again) != string(body) {
			t.Fatalf("roundtrip mismatch")
		}
	})
}

func TestQueue_CorruptHeadAtRuntimeIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	rec := int64(len(encode(msg(0))))
	q, err := Open(dir, Options{MaxBytes: rec * 100, SegmentBytes: rec * 2})
	require.NoError(t, err)
	defer q.Close()
	for i := 0; i < 4; i++ {
		require.NoError(t, q.Push(msg(i)))
	}
	// Corrupt the first record of the head segment behind the queue's back.
	segs, _ := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	require.Len(t, segs, 2)
	data, err := os.ReadFile(segs[0])
	require.NoError(t, err)
	data[recHeaderLen+5] ^= 0xFF
	require.NoError(t, os.WriteFile(segs[0], data, 0o640))

	_, _, err = q.Peek()
	require.ErrorIs(t, err, ErrCorrupt)
	require.NoError(t, q.Ack(), "ack after a corrupt peek discards the segment")
	assert.Equal(t, int64(2), q.Dropped())
	assert.Equal(t, 2, q.Len())
	m, ok, err := q.Peek()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, msg(2), m)

	// Same for a single remaining segment.
	require.NoError(t, q.Ack())
	require.NoError(t, q.Ack())
	require.NoError(t, q.Push(msg(9)))
	segs, _ = filepath.Glob(filepath.Join(dir, "seg-*.log"))
	require.Len(t, segs, 1)
	data, _ = os.ReadFile(segs[0])
	data[recHeaderLen+5] ^= 0xFF
	require.NoError(t, os.WriteFile(segs[0], data, 0o640))
	_, _, err = q.Peek()
	require.ErrorIs(t, err, ErrCorrupt)
	require.NoError(t, q.Ack())
	assert.Equal(t, 0, q.Len())
	assert.Equal(t, int64(3), q.Dropped())
	require.NoError(t, q.Push(msg(10)))
	m, ok, err = q.Peek()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, msg(10), m)
}
