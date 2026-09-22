#!/usr/bin/env sh
# Walks the read API of a running plantstream node end to end.
# Usage: examples/quickstart.sh [base-url]   (default http://localhost:8080)
set -eu
BASE="${1:-http://localhost:8080}"

step() { printf '\n\033[1m# %s\033[0m\n' "$1"; }

step "Readiness (plant loaded + broker connected)"
curl -sS "$BASE/readyz"; echo

step "Asset model with ISA-95 context and topics"
curl -sS "$BASE/v1/assets" | head -c 900; echo " ..."

step "Latest contextualised values of filler-01 (value + quality + engineering range)"
curl -sS "$BASE/v1/tags/filler-01/latest"; echo

step "Unified Namespace topics (enterprise/site/area/line/cell/tag) with current quality"
curl -sS "$BASE/v1/topics" | head -c 700; echo " ..."

step "Per-source health"
curl -sS "$BASE/v1/health/sources"; echo

step "Live stream: 3 seconds of Sparkplug-style DATA messages for every 'speed' tag"
curl -sS -N --max-time 3 "$BASE/v1/stream?topic=acme/austin/%2B/%2B/%2B/speed" || true
echo
