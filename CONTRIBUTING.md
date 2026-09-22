# Contributing

Thanks for taking the time. This project is small and opinionated; the rules
below keep it that way.

## Ground rules

- Go 1.26+, standard library first. New third-party dependencies need an ADR.
- Domain packages (`internal/domain/...`) stay free of I/O.
- Every behaviour change ships with a table-driven test; protocol and codec
  changes also need a fuzz or golden test.
- `make test lint vet` must pass before you open a pull request.
- Non-trivial design decisions are recorded in `docs/adr/` (copy the template
  of an existing ADR, number it sequentially).

## Workflow

1. Fork and branch from `main` (`feat/...`, `fix/...`).
2. Keep commits focused; write the *why* in the message body.
3. Open a PR. CI runs build, vet, race tests, golangci-lint, govulncheck, a
   container build and `helm lint`.
4. One approving review from a code owner merges.

## Local development

```sh
make run            # zero-infrastructure edge node with simulated sources
make run-modbus     # plc-sim (Modbus TCP) + edge node
make run-full       # docker compose: Mosquitto + Kafka + OTel collector
make cover          # coverage report
```
