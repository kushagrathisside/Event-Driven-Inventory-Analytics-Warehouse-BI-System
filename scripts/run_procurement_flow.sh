#!/usr/bin/env bash
# scripts/run_procurement_flow.sh — run the full procurement pipeline end-to-end.
#
# Usage:
#   ./scripts/run_procurement_flow.sh [options] [event_count]
#
# Options:
#   --bootstrap     Run warehouse bootstrap before the pipeline (required on first run)
#   --no-bronze     Skip Bronze management — assume output already exists
#   --help          Show this message
set -euo pipefail

# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

EVENT_COUNT=15
BOOTSTRAP=0
MANAGE_BRONZE=1

for arg in "$@"; do
  case "$arg" in
    --bootstrap)  BOOTSTRAP=1     ;;
    --no-bronze)  MANAGE_BRONZE=0 ;;
    --help|-h)    grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    [0-9]*)       EVENT_COUNT="$arg" ;;
    *) fail "Unknown option: $arg  (run with --help for usage)" ;;
  esac
done

main() {
  header "Procurement Pipeline"
  load_env; set_pythonpath; cd "$MW_ROOT"

  if (( BOOTSTRAP )); then
    log "Bootstrapping analytics warehouse..."; python -m medwarehouse warehouse bootstrap; ok "Bootstrap complete."
  fi

  if (( MANAGE_BRONZE )); then _run_with_managed_bronze
  else log "Skipping Bronze (--no-bronze)"; _run_silver_and_gold
  fi
}

_run_with_managed_bronze() {
  local bronze_pid="" bronze_dir="${MW_ROOT}/data/bronze/procurement_events"
  log "Starting Bronze stream in background..."
  python -m medwarehouse spark procurement-bronze &
  bronze_pid=$!
  trap '_stop_bronze "$bronze_pid"' EXIT

  sleep 3
  log "Producing ${EVENT_COUNT} procurement lifecycle events..."
  python -m medwarehouse produce procurement --max-events "$EVENT_COUNT"
  ok "Events produced."

  wait_for_parquet "$bronze_dir" 90
  sleep 2

  _stop_bronze "$bronze_pid"; trap - EXIT
  _run_silver_and_gold
}

_stop_bronze() {
  local pid="${1:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    log "Stopping Bronze (pid ${pid})..."
    kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
  fi
}

_run_silver_and_gold() {
  log "Running Silver transformation..."
  python -m medwarehouse spark procurement-silver
  log "Building Gold pipeline..."
  python -m medwarehouse orchestration build-procurement-gold
  ok "Procurement pipeline complete."
  printf "\n  Fact table: analytics.fact_procurement_events\n  BI view:    analytics.v_po_lifecycle\n\n"
}

main "$@"
