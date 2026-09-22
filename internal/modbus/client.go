package modbus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// Client is a Modbus TCP client bound to one TCP connection. It is safe for
// concurrent use; requests are serialised because Modbus TCP servers commonly
// process one transaction at a time and many PLCs misbehave when pipelined.
type Client struct {
	mu      sync.Mutex
	conn    net.Conn
	timeout time.Duration
	tid     uint16
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithTimeout sets the per-transaction I/O timeout (default 2s).
func WithTimeout(d time.Duration) ClientOption {
	return func(c *Client) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// Dial connects to a Modbus TCP server.
func Dial(ctx context.Context, addr string, opts ...ClientOption) (*Client, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("modbus: dial %s: %w", addr, err)
	}
	return NewClient(conn, opts...), nil
}

// NewClient wraps an established connection.
func NewClient(conn net.Conn, opts ...ClientOption) *Client {
	c := &Client{conn: conn, timeout: 2 * time.Second}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close()
}

// ReadHoldingRegisters performs FC 3.
func (c *Client) ReadHoldingRegisters(ctx context.Context, unit byte, addr, qty uint16) ([]uint16, error) {
	return c.readRegisters(ctx, FuncReadHoldingRegisters, unit, addr, qty)
}

// ReadInputRegisters performs FC 4.
func (c *Client) ReadInputRegisters(ctx context.Context, unit byte, addr, qty uint16) ([]uint16, error) {
	return c.readRegisters(ctx, FuncReadInputRegisters, unit, addr, qty)
}

// ReadRegisters performs FC 3 or FC 4 depending on fn.
func (c *Client) ReadRegisters(ctx context.Context, fn, unit byte, addr, qty uint16) ([]uint16, error) {
	return c.readRegisters(ctx, fn, unit, addr, qty)
}

func (c *Client) readRegisters(ctx context.Context, fn, unit byte, addr, qty uint16) ([]uint16, error) {
	req, err := ReadRegistersRequest(fn, addr, qty)
	if err != nil {
		return nil, err
	}
	resp, err := c.transact(ctx, unit, req)
	if err != nil {
		return nil, err
	}
	return ParseReadRegistersResponse(resp, fn, qty)
}

// transact sends one request and reads the matching response.
func (c *Client) transact(ctx context.Context, unit byte, req PDU) (PDU, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.tid++
	adu := ADU{TransactionID: c.tid, UnitID: unit, PDU: req}
	frame, err := adu.MarshalBinary()
	if err != nil {
		return PDU{}, err
	}

	deadline := time.Now().Add(c.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return PDU{}, fmt.Errorf("modbus: set deadline: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return PDU{}, err
	}
	if _, err := c.conn.Write(frame); err != nil {
		return PDU{}, fmt.Errorf("modbus: write: %w", err)
	}
	// Discard stale responses (e.g. from a previous timed-out transaction).
	for {
		resp, err := ReadADU(c.conn)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return PDU{}, fmt.Errorf("modbus: read: %w", context.DeadlineExceeded)
			}
			return PDU{}, fmt.Errorf("modbus: read: %w", err)
		}
		if resp.TransactionID == adu.TransactionID {
			return resp.PDU, nil
		}
	}
}
