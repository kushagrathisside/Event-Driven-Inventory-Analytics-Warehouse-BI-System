#!/usr/bin/env bash
# scripts/stop_services.sh — stop the medwarehouse Docker stack.
#
# Usage:
#   ./scripts/stop_services.sh [--volumes]
#
# Options:
#   --volumes   Also remove named volumes (destroys all data — use with care)
#   --help      Show this message
set -euo pipefail

# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

DOWN_FLAGS=()

for arg in "$@"; do
  case "$arg" in
    --volumes) DOWN_FLAGS+=(--volumes) ;;
    --help|-h)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) fail "Unknown option: $arg  (run with --help for usage)" ;;
  esac
done

header "MedWarehouse — Stopping Services"
detect_compose
cd "$MW_ROOT"

if [[ ${#DOWN_FLAGS[@]} -gt 0 ]]; then
  warn "Removing volumes — all PostgreSQL and Kafka data will be lost."
fi

compose down "${DOWN_FLAGS[@]}"
ok "All services stopped."
