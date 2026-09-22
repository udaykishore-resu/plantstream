# ADR 0005: OPC UA client is out of scope for this repository

- Status: Accepted
- Date: 2026-03-09

## Context

OPC UA is the other protocol a plant data platform is expected to speak. A
compliant client needs binary encoding of the full type system, secure channel
handshake (RSA/AES, certificates), session management, subscriptions with
monitored items, and browse/translate services. Mature Go implementations
exist (`github.com/gopcua/opcua`), but they are not on this repository's
approved dependency list and a home-grown client would dwarf the rest of the
codebase without adding architectural insight.

## Decision

OPC UA is **not implemented here**. The `Source` interface in
`internal/collectors` is the integration point: an OPC UA source would map
monitored items to tags and hand `Reading`s to the same `Poller`, quality
engine and publisher. plant.yaml already rejects unknown source types with a
clear error, so a misconfiguration fails fast.

Modbus TCP was chosen as the implemented protocol because it is small enough
to own end to end (framing, FC 3/4, exceptions, register decoding with all
four byte orders), is still the most common brownfield protocol, and lets us
ship a self-contained PLC simulator for the demo and the tests.

## Consequences

- Sites with OPC UA-only equipment need an external OPC UA → MQTT gateway
  publishing into the UNS, or a follow-up that adds a `type: opcua` source
  backed by `gopcua`. Both slot in without touching the pipeline.
- The README states the limitation up front.
