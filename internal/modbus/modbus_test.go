package modbus

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecode_Table(t *testing.T) {
	tests := []struct {
		name  string
		typ   DataType
		order ByteOrder
		regs  []uint16
		want  float64
	}{
		{"uint16 big", UInt16, ABCD, []uint16{0x1234}, 0x1234},
		{"uint16 swapped", UInt16, DCBA, []uint16{0x1234}, 0x3412},
		{"int16 negative", Int16, ABCD, []uint16{0xFFFE}, -2},
		{"int16 swapped negative", Int16, BADC, []uint16{0xFEFF}, -2},
		{"int32 ABCD", Int32, ABCD, []uint16{0xFFFF, 0xFFFE}, -2},
		{"uint32 ABCD", UInt32, ABCD, []uint16{0x0001, 0x0000}, 65536},
		{"uint32 CDAB", UInt32, CDAB, []uint16{0x0000, 0x0001}, 65536},
		{"uint32 DCBA", UInt32, DCBA, []uint16{0x0000, 0x0100}, 65536},
		{"uint32 BADC", UInt32, BADC, []uint16{0x0100, 0x0000}, 65536},
		// 1.0f = 0x3F800000
		{"float32 ABCD", Float32, ABCD, []uint16{0x3F80, 0x0000}, 1},
		{"float32 CDAB", Float32, CDAB, []uint16{0x0000, 0x3F80}, 1},
		{"float32 DCBA", Float32, DCBA, []uint16{0x0000, 0x803F}, 1},
		{"float32 BADC", Float32, BADC, []uint16{0x803F, 0x0000}, 1},
		// -123.456f = 0xC2F6E979
		{"float32 negative", Float32, ABCD, []uint16{0xC2F6, 0xE979}, float64(float32(-123.456))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Decode(tc.typ, tc.order, tc.regs)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDecode_WrongWordCount(t *testing.T) {
	_, err := Decode(Float32, ABCD, []uint16{1})
	require.Error(t, err)
	_, err = Decode(Int16, ABCD, []uint16{1, 2})
	require.Error(t, err)
	_, err = Decode(DataType("bogus"), ABCD, []uint16{1})
	require.Error(t, err)
}

func TestEncodeDecode_Roundtrip(t *testing.T) {
	values := []float64{0, 1, -1, 12345, -32768, 32767, 65535, 2147483647, -2147483648, 4294967295, 3.5, -0.25, 1e10, -1e10, math.Inf(1), math.NaN()}
	for _, typ := range []DataType{Int16, UInt16, Int32, UInt32, Float32} {
		for _, order := range []ByteOrder{ABCD, DCBA, BADC, CDAB} {
			for _, v := range values {
				regs, err := Encode(typ, order, v)
				require.NoError(t, err)
				require.Len(t, regs, int(typ.Words()))
				got, err := Decode(typ, order, regs)
				require.NoError(t, err)
				want := expected(typ, v)
				if math.IsNaN(want) {
					assert.True(t, math.IsNaN(got), "%s/%s/%v", typ, order, v)
				} else {
					assert.Equal(t, want, got, "%s/%s/%v", typ, order, v)
				}
			}
		}
	}
}

// expected mirrors Encode's clamping semantics for integer types.
func expected(typ DataType, v float64) float64 {
	switch typ {
	case Int16:
		return clamp(v, math.MinInt16, math.MaxInt16)
	case UInt16:
		return clamp(v, 0, math.MaxUint16)
	case Int32:
		return clamp(v, math.MinInt32, math.MaxInt32)
	case UInt32:
		return clamp(v, 0, math.MaxUint32)
	default:
		return float64(float32(v))
	}
}

func TestParse(t *testing.T) {
	for _, s := range []string{"int16", "uint16", "int32", "uint32", "float32"} {
		_, err := ParseDataType(s)
		assert.NoError(t, err)
	}
	_, err := ParseDataType("int64")
	assert.Error(t, err)
	_, err = ParseDataType("")
	assert.Error(t, err)

	o, err := ParseByteOrder("")
	require.NoError(t, err)
	assert.Equal(t, ABCD, o)
	_, err = ParseByteOrder("ACBD")
	assert.Error(t, err)
}

func TestADU_Roundtrip(t *testing.T) {
	req, err := ReadRegistersRequest(FuncReadHoldingRegisters, 100, 10)
	require.NoError(t, err)
	adu := ADU{TransactionID: 0xBEEF, UnitID: 7, PDU: req}
	frame, err := adu.MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, []byte{0xBE, 0xEF, 0, 0, 0, 6, 7, 3, 0, 100, 0, 10}, frame)

	got, err := ReadADU(bytes.NewReader(frame))
	require.NoError(t, err)
	assert.Equal(t, adu, got)

	addr, qty, err := ParseReadRegistersRequest(got.PDU)
	require.NoError(t, err)
	assert.Equal(t, uint16(100), addr)
	assert.Equal(t, uint16(10), qty)
}

func TestReadRegistersRequest_Validation(t *testing.T) {
	_, err := ReadRegistersRequest(FuncReadHoldingRegisters, 0, 0)
	assert.ErrorIs(t, err, ErrQuantity)
	_, err = ReadRegistersRequest(FuncReadHoldingRegisters, 0, 126)
	assert.ErrorIs(t, err, ErrQuantity)
	_, err = ReadRegistersRequest(FuncReadHoldingRegisters, 65535, 2)
	assert.ErrorIs(t, err, ErrAddressOverflow)
	_, err = ReadRegistersRequest(0x10, 0, 1)
	var ex *Exception
	assert.ErrorAs(t, err, &ex)
}

func TestReadADU_Errors(t *testing.T) {
	_, err := ReadADU(bytes.NewReader([]byte{0, 1, 0, 1, 0, 2, 1, 3}))
	assert.ErrorIs(t, err, ErrBadProtocolID)
	_, err = ReadADU(bytes.NewReader([]byte{0, 1, 0, 0, 0, 1, 1}))
	assert.ErrorIs(t, err, ErrShortFrame)
	_, err = ReadADU(bytes.NewReader([]byte{0, 1, 0, 0, 0xFF, 0xFF, 1}))
	assert.ErrorIs(t, err, ErrFrameTooLarge)
	_, err = ReadADU(bytes.NewReader([]byte{0, 1, 0, 0, 0, 6, 1, 3}))
	assert.Error(t, err) // truncated body
}

func TestParseReadRegistersResponse(t *testing.T) {
	resp := ReadRegistersResponse(FuncReadInputRegisters, []uint16{1, 2, 3})
	regs, err := ParseReadRegistersResponse(resp, FuncReadInputRegisters, 3)
	require.NoError(t, err)
	assert.Equal(t, []uint16{1, 2, 3}, regs)

	_, err = ParseReadRegistersResponse(resp, FuncReadInputRegisters, 2)
	assert.ErrorIs(t, err, ErrByteCount)

	_, err = ParseReadRegistersResponse(resp, FuncReadHoldingRegisters, 3)
	assert.ErrorIs(t, err, ErrUnexpectedFunc)

	_, err = ParseReadRegistersResponse(ExceptionResponse(FuncReadInputRegisters, ExIllegalDataAddress), FuncReadInputRegisters, 3)
	var ex *Exception
	require.ErrorAs(t, err, &ex)
	assert.Equal(t, ExIllegalDataAddress, ex.Code)
	assert.Contains(t, ex.Error(), "illegal data address")

	_, err = ParseReadRegistersResponse(PDU{Function: FuncReadInputRegisters}, FuncReadInputRegisters, 1)
	assert.ErrorIs(t, err, ErrShortFrame)
}

func TestExceptionCode_String(t *testing.T) {
	assert.Equal(t, "illegal function", ExIllegalFunction.String())
	assert.Equal(t, "exception 0x7f", ExceptionCode(0x7f).String())
}

func FuzzReadADU(f *testing.F) {
	frame, _ := ADU{TransactionID: 1, UnitID: 1, PDU: PDU{Function: 3, Data: []byte{0, 0, 0, 1}}}.MarshalBinary()
	f.Add(frame)
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		adu, err := ReadADU(bytes.NewReader(data))
		if err != nil {
			return
		}
		// Anything that parses must re-serialise to a prefix of the input.
		out, err := adu.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal parsed frame: %v", err)
		}
		if !bytes.HasPrefix(data, out) {
			t.Fatalf("roundtrip mismatch: in=%x out=%x", data, out)
		}
		_, _, _ = ParseReadRegistersRequest(adu.PDU)
		_, _ = ParseReadRegistersResponse(adu.PDU, adu.PDU.Function, uint16(len(adu.PDU.Data)/2))
	})
}

func FuzzDecodeRegisters(f *testing.F) {
	f.Add(uint16(0x3F80), uint16(0), "float32", "ABCD")
	f.Add(uint16(0xFFFF), uint16(0xFFFE), "int32", "CDAB")
	f.Fuzz(func(t *testing.T, r0, r1 uint16, typ, order string) {
		dt, err := ParseDataType(typ)
		if err != nil {
			return
		}
		bo, err := ParseByteOrder(order)
		if err != nil {
			return
		}
		regs := []uint16{r0, r1}[:dt.Words()]
		v, err := Decode(dt, bo, regs)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if dt == Float32 && (math.IsNaN(v) || math.IsInf(v, 0)) {
			return
		}
		back, err := Encode(dt, bo, v)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if len(back) != len(regs) {
			t.Fatalf("length mismatch")
		}
		for i := range regs {
			if back[i] != regs[i] {
				t.Fatalf("roundtrip %s/%s: %v -> %v -> %v", dt, bo, regs, v, back)
			}
		}
	})
}

func startServer(t *testing.T, bank RegisterBank) (string, context.CancelFunc) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	srv := NewServer(bank, nil)
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(ctx, ln)
		close(done)
	}()
	return ln.Addr().String(), func() {
		cancel()
		<-done
	}
}

func TestClientServer_Loopback(t *testing.T) {
	bank := NewMemoryBank(64, 16)
	regs, err := Encode(Float32, ABCD, 480.5)
	require.NoError(t, err)
	require.NoError(t, bank.SetHolding(10, regs))
	require.NoError(t, bank.SetInput(0, []uint16{7, 8}))

	addr, stop := startServer(t, bank)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr, WithTimeout(time.Second))
	require.NoError(t, err)
	defer c.Close()

	got, err := c.ReadHoldingRegisters(ctx, 1, 10, 2)
	require.NoError(t, err)
	v, err := Decode(Float32, ABCD, got)
	require.NoError(t, err)
	assert.InDelta(t, 480.5, v, 1e-6)

	in, err := c.ReadInputRegisters(ctx, 1, 0, 2)
	require.NoError(t, err)
	assert.Equal(t, []uint16{7, 8}, in)

	in, err = c.ReadRegisters(ctx, FuncReadInputRegisters, 1, 1, 1)
	require.NoError(t, err)
	assert.Equal(t, []uint16{8}, in)

	// Out of range → exception 0x02.
	_, err = c.ReadHoldingRegisters(ctx, 1, 60, 10)
	var ex *Exception
	require.ErrorAs(t, err, &ex)
	assert.Equal(t, ExIllegalDataAddress, ex.Code)

	// Invalid quantity is rejected client-side.
	_, err = c.ReadHoldingRegisters(ctx, 1, 0, 0)
	assert.ErrorIs(t, err, ErrQuantity)

	// Second transaction after an exception still works (tid tracking).
	got, err = c.ReadHoldingRegisters(ctx, 1, 10, 2)
	require.NoError(t, err)
	assert.Len(t, got, 2)
}

func TestServer_IllegalFunction(t *testing.T) {
	addr, stop := startServer(t, NewMemoryBank(8, 8))
	defer stop()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	frame, _ := ADU{TransactionID: 9, UnitID: 1, PDU: PDU{Function: 0x06, Data: []byte{0, 0, 0, 1}}}.MarshalBinary()
	_, err = conn.Write(frame)
	require.NoError(t, err)
	resp, err := ReadADU(conn)
	require.NoError(t, err)
	assert.Equal(t, uint16(9), resp.TransactionID)
	assert.Equal(t, byte(0x86), resp.PDU.Function)
	assert.Equal(t, []byte{byte(ExIllegalFunction)}, resp.PDU.Data)

	// Malformed FC3 request → illegal data value.
	frame, _ = ADU{TransactionID: 10, UnitID: 1, PDU: PDU{Function: 0x03, Data: []byte{0, 0}}}.MarshalBinary()
	_, err = conn.Write(frame)
	require.NoError(t, err)
	resp, err = ReadADU(conn)
	require.NoError(t, err)
	assert.Equal(t, []byte{byte(ExIllegalDataValue)}, resp.PDU.Data)
}

type failingBank struct{}

func (failingBank) ReadHolding(byte, uint16, uint16) ([]uint16, error) {
	return nil, errors.New("boom")
}
func (failingBank) ReadInput(byte, uint16, uint16) ([]uint16, error) {
	return []uint16{1}, nil // wrong length for qty 2
}

func TestServer_BankFailures(t *testing.T) {
	addr, stop := startServer(t, failingBank{})
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr)
	require.NoError(t, err)
	defer c.Close()
	_, err = c.ReadHoldingRegisters(ctx, 1, 0, 1)
	var ex *Exception
	require.ErrorAs(t, err, &ex)
	assert.Equal(t, ExServerDeviceFailure, ex.Code)
	_, err = c.ReadInputRegisters(ctx, 1, 0, 2)
	require.ErrorAs(t, err, &ex)
	assert.Equal(t, ExServerDeviceFailure, ex.Code)
}

func TestClient_Timeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			defer conn.Close()
			time.Sleep(500 * time.Millisecond) // never answer
		}
	}()
	ctx := context.Background()
	c, err := Dial(ctx, ln.Addr().String(), WithTimeout(50*time.Millisecond))
	require.NoError(t, err)
	defer c.Close()
	_, err = c.ReadHoldingRegisters(ctx, 1, 0, 1)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMemoryBank_WriteBounds(t *testing.T) {
	b := NewMemoryBank(4, 4)
	assert.Error(t, b.SetHolding(3, []uint16{1, 2}))
	assert.Error(t, b.SetInput(4, []uint16{1}))
	assert.NoError(t, b.SetInput(2, []uint16{1, 2}))
	got, err := b.ReadInput(0, 2, 2)
	require.NoError(t, err)
	assert.Equal(t, []uint16{1, 2}, got)
}
