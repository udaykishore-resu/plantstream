// Package modbus implements the subset of Modbus TCP needed by plantstream:
// MBAP framing, function codes 3 (Read Holding Registers) and 4 (Read Input
// Registers), exception responses, a client and a server. The register codec
// (int16/uint16/int32/uint32/float32 with byte-order options) lives in regs.go.
//
// The implementation follows MODBUS Application Protocol Specification V1.1b3
// and MODBUS Messaging on TCP/IP Implementation Guide V1.0b. No third-party
// dependency is used; the whole protocol surface is small enough to own.
package modbus

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Function codes supported by this package.
const (
	FuncReadHoldingRegisters byte = 0x03
	FuncReadInputRegisters   byte = 0x04

	// exceptionFlag is OR-ed into the function code of an exception response.
	exceptionFlag byte = 0x80

	// MaxRegistersPerRead is the protocol limit for FC 3/4 (spec: 1..125).
	MaxRegistersPerRead uint16 = 125

	// MaxPDU is the maximum PDU length (function code + data) per spec.
	MaxPDU = 253

	mbapHeaderLen = 7
	protocolID    = 0
)

// ExceptionCode is a Modbus exception code returned by a server.
type ExceptionCode byte

// Standard exception codes.
const (
	ExIllegalFunction     ExceptionCode = 0x01
	ExIllegalDataAddress  ExceptionCode = 0x02
	ExIllegalDataValue    ExceptionCode = 0x03
	ExServerDeviceFailure ExceptionCode = 0x04
	ExGatewayTargetFailed ExceptionCode = 0x0B
)

func (c ExceptionCode) String() string {
	switch c {
	case ExIllegalFunction:
		return "illegal function"
	case ExIllegalDataAddress:
		return "illegal data address"
	case ExIllegalDataValue:
		return "illegal data value"
	case ExServerDeviceFailure:
		return "server device failure"
	case ExGatewayTargetFailed:
		return "gateway target device failed to respond"
	default:
		return fmt.Sprintf("exception 0x%02x", byte(c))
	}
}

// Exception is the error returned when the server answers with an exception PDU.
type Exception struct {
	Function byte
	Code     ExceptionCode
}

func (e *Exception) Error() string {
	return fmt.Sprintf("modbus: function 0x%02x: %s (0x%02x)", e.Function, e.Code, byte(e.Code))
}

// Protocol-level errors.
var (
	ErrShortFrame      = errors.New("modbus: short frame")
	ErrBadProtocolID   = errors.New("modbus: unexpected protocol identifier")
	ErrFrameTooLarge   = errors.New("modbus: frame exceeds maximum PDU size")
	ErrTransactionID   = errors.New("modbus: transaction id mismatch")
	ErrUnexpectedFunc  = errors.New("modbus: unexpected function code in response")
	ErrByteCount       = errors.New("modbus: byte count does not match requested quantity")
	ErrQuantity        = errors.New("modbus: register quantity out of range (1..125)")
	ErrAddressOverflow = errors.New("modbus: start address + quantity overflows 16 bits")
)

// PDU is a protocol data unit: a function code plus its data.
type PDU struct {
	Function byte
	Data     []byte
}

// ADU is a Modbus TCP application data unit (MBAP header + PDU).
type ADU struct {
	TransactionID uint16
	UnitID        byte
	PDU           PDU
}

// MarshalBinary encodes the ADU as an MBAP frame.
func (a ADU) MarshalBinary() ([]byte, error) {
	if len(a.PDU.Data)+1 > MaxPDU {
		return nil, ErrFrameTooLarge
	}
	buf := make([]byte, mbapHeaderLen+1+len(a.PDU.Data))
	binary.BigEndian.PutUint16(buf[0:2], a.TransactionID)
	binary.BigEndian.PutUint16(buf[2:4], protocolID)
	// Length counts unit id + function code + data.
	binary.BigEndian.PutUint16(buf[4:6], uint16(2+len(a.PDU.Data)))
	buf[6] = a.UnitID
	buf[7] = a.PDU.Function
	copy(buf[8:], a.PDU.Data)
	return buf, nil
}

// ReadADU reads exactly one MBAP frame from r.
func ReadADU(r io.Reader) (ADU, error) {
	var hdr [mbapHeaderLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return ADU{}, err
	}
	if binary.BigEndian.Uint16(hdr[2:4]) != protocolID {
		return ADU{}, ErrBadProtocolID
	}
	length := int(binary.BigEndian.Uint16(hdr[4:6]))
	if length < 2 {
		return ADU{}, ErrShortFrame
	}
	if length-1 > MaxPDU {
		return ADU{}, ErrFrameTooLarge
	}
	body := make([]byte, length-1) // function code + data
	if _, err := io.ReadFull(r, body); err != nil {
		return ADU{}, err
	}
	return ADU{
		TransactionID: binary.BigEndian.Uint16(hdr[0:2]),
		UnitID:        hdr[6],
		PDU:           PDU{Function: body[0], Data: body[1:]},
	}, nil
}

// ReadRegistersRequest builds the PDU for FC 3 or 4.
func ReadRegistersRequest(fn byte, addr, qty uint16) (PDU, error) {
	if fn != FuncReadHoldingRegisters && fn != FuncReadInputRegisters {
		return PDU{}, &Exception{Function: fn, Code: ExIllegalFunction}
	}
	if qty == 0 || qty > MaxRegistersPerRead {
		return PDU{}, ErrQuantity
	}
	if uint32(addr)+uint32(qty) > 0x10000 {
		return PDU{}, ErrAddressOverflow
	}
	data := make([]byte, 4)
	binary.BigEndian.PutUint16(data[0:2], addr)
	binary.BigEndian.PutUint16(data[2:4], qty)
	return PDU{Function: fn, Data: data}, nil
}

// ParseReadRegistersRequest decodes a FC 3/4 request PDU into (addr, qty).
func ParseReadRegistersRequest(p PDU) (addr, qty uint16, err error) {
	if len(p.Data) != 4 {
		return 0, 0, ErrShortFrame
	}
	addr = binary.BigEndian.Uint16(p.Data[0:2])
	qty = binary.BigEndian.Uint16(p.Data[2:4])
	if qty == 0 || qty > MaxRegistersPerRead {
		return 0, 0, ErrQuantity
	}
	if uint32(addr)+uint32(qty) > 0x10000 {
		return 0, 0, ErrAddressOverflow
	}
	return addr, qty, nil
}

// ReadRegistersResponse builds the PDU answering a FC 3/4 request.
func ReadRegistersResponse(fn byte, regs []uint16) PDU {
	data := make([]byte, 1+2*len(regs))
	data[0] = byte(2 * len(regs))
	for i, r := range regs {
		binary.BigEndian.PutUint16(data[1+2*i:], r)
	}
	return PDU{Function: fn, Data: data}
}

// ParseReadRegistersResponse validates and decodes a FC 3/4 response PDU.
// wantFn is the function code that was requested; wantQty the quantity.
func ParseReadRegistersResponse(p PDU, wantFn byte, wantQty uint16) ([]uint16, error) {
	if p.Function == wantFn|exceptionFlag {
		code := ExServerDeviceFailure
		if len(p.Data) >= 1 {
			code = ExceptionCode(p.Data[0])
		}
		return nil, &Exception{Function: wantFn, Code: code}
	}
	if p.Function != wantFn {
		return nil, fmt.Errorf("%w: got 0x%02x want 0x%02x", ErrUnexpectedFunc, p.Function, wantFn)
	}
	if len(p.Data) < 1 {
		return nil, ErrShortFrame
	}
	n := int(p.Data[0])
	if n != 2*int(wantQty) || len(p.Data)-1 != n {
		return nil, fmt.Errorf("%w: byte count %d, payload %d, want %d", ErrByteCount, n, len(p.Data)-1, 2*wantQty)
	}
	regs := make([]uint16, wantQty)
	for i := range regs {
		regs[i] = binary.BigEndian.Uint16(p.Data[1+2*i:])
	}
	return regs, nil
}

// ExceptionResponse builds an exception PDU for the given request function.
func ExceptionResponse(fn byte, code ExceptionCode) PDU {
	return PDU{Function: fn | exceptionFlag, Data: []byte{byte(code)}}
}
