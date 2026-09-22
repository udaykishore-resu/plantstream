# Security policy

## Supported versions

The `main` branch and the latest tagged release receive security fixes.

## Reporting a vulnerability

Please do not open public issues for security problems. Use GitHub's private
vulnerability reporting on this repository ("Report a vulnerability" under the
Security tab). You will receive an acknowledgement within 72 hours and a fix or
mitigation plan within 14 days for confirmed issues.

## Threat model notes

- plantstream runs at the OT/IT boundary. It only ever *reads* from field
  devices (Modbus FC 3/4); it never writes registers.
- The HTTP API is read-only and unauthenticated by design; expose it only on
  the plant network or behind an authenticating ingress (see `docs/runbook.md`).
- MQTT and Kafka credentials are supplied through environment variables sourced
  from Kubernetes Secrets (`deploy/helm`), never from the repository.
- The container image is distroless, runs as non-root with a read-only root
  filesystem and no Linux capabilities.
