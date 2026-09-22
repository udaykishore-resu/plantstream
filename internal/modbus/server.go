package modbus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// RegisterBank is the data source behind a Server. Implementations return an
// *ExceptionError to signal a protocol-level error (e.g. ExIllegalDataAddress).
type RegisterBank interface {
	ReadHolding(unit byte, addr, qty uint16) ([]uint16, error)
	ReadInput(unit byte, addr, qty uint16) ([]uint16, error)
}

// Server is a minimal Modbus TCP server (FC 3/4 only). It exists to power
// cmd/plc-sim and the client's tests; it is not intended as a general PLC.
type Server struct {
	bank RegisterBank
	log  *slog.Logger

	idleTimeout time.Duration
	wg          sync.WaitGroup
}

// NewServer creates a server backed by bank.
func NewServer(bank RegisterBank, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{bank: bank, log: log, idleTimeout: 5 * time.Minute}
}

// Serve accepts connections on ln until ctx is cancelled, then closes the
// listener and waits for in-flight connections to finish.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				// The listener was closed by our own ctx watcher: a clean shutdown, not an error.
				s.wg.Wait()
				return nil //nolint:nilerr // accept error caused by intentional listener close on ctx cancellation
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			s.wg.Wait()
			return fmt.Errorf("modbus: accept: %w", err)
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(ctx, conn)
		}()
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }() // may already be closed by the ctx watcher below
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	for {
		if err := conn.SetReadDeadline(time.Now().Add(s.idleTimeout)); err != nil {
			return
		}
		req, err := ReadADU(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				var ne net.Error
				if !errors.As(err, &ne) || !ne.Timeout() {
					s.log.Debug("modbus: connection closed", "remote", conn.RemoteAddr().String(), "err", err)
				}
			}
			return
		}
		resp := ADU{TransactionID: req.TransactionID, UnitID: req.UnitID, PDU: s.dispatch(req.UnitID, req.PDU)}
		frame, err := resp.MarshalBinary()
		if err != nil {
			return
		}
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return
		}
		if _, err := conn.Write(frame); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(unit byte, req PDU) PDU {
	switch req.Function {
	case FuncReadHoldingRegisters, FuncReadInputRegisters:
		addr, qty, err := ParseReadRegistersRequest(req)
		if err != nil {
			return ExceptionResponse(req.Function, ExIllegalDataValue)
		}
		var regs []uint16
		if req.Function == FuncReadHoldingRegisters {
			regs, err = s.bank.ReadHolding(unit, addr, qty)
		} else {
			regs, err = s.bank.ReadInput(unit, addr, qty)
		}
		if err != nil {
			var ex *ExceptionError
			if errors.As(err, &ex) {
				return ExceptionResponse(req.Function, ex.Code)
			}
			return ExceptionResponse(req.Function, ExServerDeviceFailure)
		}
		if len(regs) != int(qty) {
			return ExceptionResponse(req.Function, ExServerDeviceFailure)
		}
		return ReadRegistersResponse(req.Function, regs)
	default:
		return ExceptionResponse(req.Function, ExIllegalFunction)
	}
}

// MemoryBank is a thread-safe RegisterBank with fixed-size holding and input
// register tables. Unit IDs are ignored (single-device simulator).
type MemoryBank struct {
	mu      sync.RWMutex
	holding []uint16
	input   []uint16
}

// NewMemoryBank allocates holding and input tables of the given sizes.
func NewMemoryBank(holdingSize, inputSize int) *MemoryBank {
	return &MemoryBank{holding: make([]uint16, holdingSize), input: make([]uint16, inputSize)}
}

// ReadHolding implements RegisterBank.
func (b *MemoryBank) ReadHolding(_ byte, addr, qty uint16) ([]uint16, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return slice(b.holding, addr, qty, FuncReadHoldingRegisters)
}

// ReadInput implements RegisterBank.
func (b *MemoryBank) ReadInput(_ byte, addr, qty uint16) ([]uint16, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return slice(b.input, addr, qty, FuncReadInputRegisters)
}

// SetHolding writes regs starting at addr into the holding table.
func (b *MemoryBank) SetHolding(addr uint16, regs []uint16) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return write(b.holding, addr, regs)
}

// SetInput writes regs starting at addr into the input table.
func (b *MemoryBank) SetInput(addr uint16, regs []uint16) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return write(b.input, addr, regs)
}

func slice(table []uint16, addr, qty uint16, fn byte) ([]uint16, error) {
	end := int(addr) + int(qty)
	if end > len(table) {
		return nil, &ExceptionError{Function: fn, Code: ExIllegalDataAddress}
	}
	out := make([]uint16, qty)
	copy(out, table[addr:end])
	return out, nil
}

func write(table []uint16, addr uint16, regs []uint16) error {
	end := int(addr) + len(regs)
	if end > len(table) {
		return fmt.Errorf("modbus: write at %d+%d exceeds table size %d", addr, len(regs), len(table))
	}
	copy(table[addr:end], regs)
	return nil
}
