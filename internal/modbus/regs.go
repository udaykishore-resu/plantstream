package modbus

import (
	"encoding/binary"
	"fmt"
	"math"
)

// DataType is the on-the-wire register data type of a tag.
type DataType string

// Supported register data types.
const (
	Int16   DataType = "int16"
	UInt16  DataType = "uint16"
	Int32   DataType = "int32"
	UInt32  DataType = "uint32"
	Float32 DataType = "float32"
)

// ByteOrder describes how the bytes of a 32-bit value are laid out across two
// 16-bit registers, using the conventional A B C D notation where A is the
// most significant byte of the logical value.
//
//	ABCD  big-endian            (Modbus default; "big")
//	DCBA  little-endian          (fully reversed)
//	BADC  big-endian byte swap   (bytes swapped within each word)
//	CDAB  little-endian word swap (words swapped, bytes in order)
//
// For 16-bit values only the byte order within the word matters: ABCD and CDAB
// mean big-endian; DCBA and BADC mean the two bytes are swapped.
type ByteOrder string

// Supported byte orders.
const (
	ABCD ByteOrder = "ABCD"
	DCBA ByteOrder = "DCBA"
	BADC ByteOrder = "BADC"
	CDAB ByteOrder = "CDAB"
)

// ParseDataType validates a data type string.
func ParseDataType(s string) (DataType, error) {
	switch DataType(s) {
	case Int16, UInt16, Int32, UInt32, Float32:
		return DataType(s), nil
	case "":
		return "", fmt.Errorf("modbus: data type is required")
	default:
		return "", fmt.Errorf("modbus: unsupported data type %q (want int16|uint16|int32|uint32|float32)", s)
	}
}

// ParseByteOrder validates a byte order string; empty defaults to ABCD.
func ParseByteOrder(s string) (ByteOrder, error) {
	switch ByteOrder(s) {
	case "", ABCD:
		return ABCD, nil
	case DCBA, BADC, CDAB:
		return ByteOrder(s), nil
	default:
		return "", fmt.Errorf("modbus: unsupported byte order %q (want ABCD|DCBA|BADC|CDAB)", s)
	}
}

// Words returns the number of 16-bit registers the type occupies.
func (t DataType) Words() uint16 {
	switch t {
	case Int32, UInt32, Float32:
		return 2
	default:
		return 1
	}
}

// canonical returns the value bytes in logical big-endian (ABCD) order.
func canonical(order ByteOrder, regs []uint16) []byte {
	if len(regs) == 1 {
		b := []byte{byte(regs[0] >> 8), byte(regs[0])}
		if order == DCBA || order == BADC {
			b[0], b[1] = b[1], b[0]
		}
		return b
	}
	a, b := byte(regs[0]>>8), byte(regs[0])
	c, d := byte(regs[1]>>8), byte(regs[1])
	switch order {
	case DCBA:
		return []byte{d, c, b, a}
	case BADC:
		return []byte{b, a, d, c}
	case CDAB:
		return []byte{c, d, a, b}
	default:
		return []byte{a, b, c, d}
	}
}

// fromCanonical is the inverse of canonical.
func fromCanonical(order ByteOrder, bytes []byte) []uint16 {
	if len(bytes) == 2 {
		if order == DCBA || order == BADC {
			return []uint16{uint16(bytes[1])<<8 | uint16(bytes[0])}
		}
		return []uint16{uint16(bytes[0])<<8 | uint16(bytes[1])}
	}
	a, b, c, d := bytes[0], bytes[1], bytes[2], bytes[3]
	var o [4]byte
	switch order {
	case DCBA:
		o = [4]byte{d, c, b, a}
	case BADC:
		o = [4]byte{b, a, d, c}
	case CDAB:
		o = [4]byte{c, d, a, b}
	default:
		o = [4]byte{a, b, c, d}
	}
	return []uint16{uint16(o[0])<<8 | uint16(o[1]), uint16(o[2])<<8 | uint16(o[3])}
}

// Decode interprets regs as a value of type t laid out in byte order o and
// returns it widened to float64. regs must contain exactly t.Words() entries.
func Decode(t DataType, o ByteOrder, regs []uint16) (float64, error) {
	if int(t.Words()) != len(regs) {
		return 0, fmt.Errorf("modbus: %s needs %d register(s), got %d", t, t.Words(), len(regs))
	}
	b := canonical(o, regs)
	switch t {
	case Int16:
		return float64(int16(binary.BigEndian.Uint16(b))), nil
	case UInt16:
		return float64(binary.BigEndian.Uint16(b)), nil
	case Int32:
		return float64(int32(binary.BigEndian.Uint32(b))), nil
	case UInt32:
		return float64(binary.BigEndian.Uint32(b)), nil
	case Float32:
		f := math.Float32frombits(binary.BigEndian.Uint32(b))
		return float64(f), nil
	default:
		return 0, fmt.Errorf("modbus: unsupported data type %q", t)
	}
}

// Encode converts v to registers of type t in byte order o. Integers are
// truncated toward zero and clamped to the type's range so a simulator never
// produces garbage for slightly out-of-range values.
func Encode(t DataType, o ByteOrder, v float64) ([]uint16, error) {
	var b []byte
	switch t {
	case Int16:
		b = make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(int16(clamp(v, math.MinInt16, math.MaxInt16))))
	case UInt16:
		b = make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(clamp(v, 0, math.MaxUint16)))
	case Int32:
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(int32(clamp(v, math.MinInt32, math.MaxInt32))))
	case UInt32:
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(clamp(v, 0, math.MaxUint32)))
	case Float32:
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, math.Float32bits(float32(v)))
	default:
		return nil, fmt.Errorf("modbus: unsupported data type %q", t)
	}
	return fromCanonical(o, b), nil
}

func clamp(v, lo, hi float64) float64 {
	if math.IsNaN(v) {
		return 0
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return math.Trunc(v)
}
