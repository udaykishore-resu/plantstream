# ADR 0006: Implement Modbus TCP in-repo instead of importing a client library

- Status: Accepted
- Date: 2026-03-09

## Context

Several Go Modbus libraries exist. The collector needs a narrow slice of the
protocol: MBAP framing, function codes 3 and 4, exception decoding and typed
register decoding with byte-order options. The demo also needs a *server*
(the PLC simulator), which most client libraries do not provide.

## Decision

`internal/modbus` implements the subset ourselves (~400 lines):

- MBAP frame encode/decode with transaction-id correlation and stale-response
  skipping; PDU size limits from the spec.
- Client with per-transaction deadlines, serialised transactions (PLCs
  misbehave when pipelined) and typed errors (`*Exception`, `ErrQuantity`, …).
- Server with a `RegisterBank` interface, used by `cmd/plc-sim` and the tests.
- Register codec for int16/uint16/int32/uint32/float32 in ABCD, DCBA, BADC and
  CDAB byte orders, with encode/decode round-trip property tests and fuzzing.
- A read planner that coalesces tags into the minimum number of requests
  (bridging gaps ≤ `max_gap`, never exceeding 125 registers).

## Consequences

- No third-party dependency for the OT protocol; the whole surface is under
  our tests (`FuzzReadADU`, `FuzzDecodeRegisters`, loopback client/server).
- Only FC 3/4 are supported. Writes (FC 6/16) are deliberately absent: the
  platform is read-only towards the plant floor (see SECURITY.md).
- Modbus RTU over serial and RTU-over-TCP framing are not supported.
