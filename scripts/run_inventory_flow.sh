#!/usr/bin/env bash
# scripts/run_inventory_flow.sh — run the full inventory pipeline end-to-end.
#
# What this script does:
#   1. Starts the Bronze stream in the background
#   2. Produces sample inventory events into Kafka
#   3. Waits for events to land in Bronze parquet output
#   4. Stops Bronze cleanly
#   5. Runs Silver transformation and the full Gold pipeline
#
# Usage:
#   ./scripts/run_inventory_flow.sh [options] [event_count]
#
# Arguments:
#   event_count     Number of events to produce (default: 10)
#
# Options:
#   --bootstrap     Run warehouse bootstrap before the pipeline (required on first run)
#   --no-bronze     Skip Bronze management — assume output already exists
#   --help          Show this message
set -euo pipefail

# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

EVENT_COUNT=10
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
  header "Inventory Pipeline"
  load_env; set_pythonpath; cd "$MW_ROOT"

  if (( BOOTSTRAP )); then
    log "Bootstrapping analytics warehouse..."; python -m medwarehouse warehouse bootstrap; ok "Bootstrap complete."
  fi

  if (( MANAGE_BRONZE )); then _run_with_managed_bronze
  else log "Skipping Bronze (--no-bronze)"; _run_silver_and_gold
  fi
}

_run_with_managed_bronze() {
  local bronze_pid="" bronze_dir="${MW_ROOT}/data/bronze/inventory_events"
  log "Starting Bronze stream in background..."
  python -m medwarehouse spark inventory-bronze &
  bronze_pid=$!
  trap '_stop_bronze "$bronze_pid"' EXIT

  sleep 3
  log "Producing ${EVENT_COUNT} inventory events..."
  python -m medwarehouse produce inventory --max-events "$EVENT_COUNT"
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
  python -m medwarehouse spark inventory-silver
  log "Building Gold pipeline..."
  python -m medwarehouse orchestration build-gold
  ok "Inventory pipeline complete."
  printf "\n  Fact table: analytics.fact_inventory_events\n  BI view:    analytics.v_inventory_snapshot\n\n"
}

main "$@"
