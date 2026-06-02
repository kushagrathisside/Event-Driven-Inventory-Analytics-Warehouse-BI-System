#!/usr/bin/env bash
# scripts/start_services.sh — start the medwarehouse Docker stack.
#
# Usage:
#   ./scripts/start_services.sh [options]
#
# Options:
#   --with-airflow   Also start Airflow (webserver + scheduler)
#   --no-pull        Skip pulling images (faster on repeat runs)
#   --help           Show this message
set -euo pipefail

# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

# ── Constants ─────────────────────────────────────────────────────────────────
readonly CORE_SERVICES=(postgres zookeeper kafka)
readonly AIRFLOW_SERVICES=(airflow-init airflow-webserver airflow-scheduler)
readonly KAFKA_TOPICS=(inventory_events procurement_events sales_events)

# ── Argument parsing ──────────────────────────────────────────────────────────
START_AIRFLOW=0
PULL_IMAGES=1

usage() {
  grep '^#' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

for arg in "$@"; do
  case "$arg" in
    --with-airflow|--all) START_AIRFLOW=1 ;;
    --core)               START_AIRFLOW=0 ;;
    --no-pull)            PULL_IMAGES=0   ;;
    --help|-h)            usage           ;;
    *) fail "Unknown option: $arg  (run with --help for usage)" ;;
  esac
done

SERVICES_TO_START=("${CORE_SERVICES[@]}")
(( START_AIRFLOW )) && SERVICES_TO_START+=("${AIRFLOW_SERVICES[@]}")

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  header "MedWarehouse — Starting Services"

  load_env
  detect_compose
  cd "$MW_ROOT"

  if (( PULL_IMAGES )); then
    log "Pulling images (pass --no-pull to skip on repeat runs)..."
    local pull_attempt=1
    until compose pull "${SERVICES_TO_START[@]}"; do
      (( pull_attempt++ ))
      (( pull_attempt > 3 )) && fail "Image pull failed after 3 attempts.
  If you are behind a proxy, set HTTP_PROXY / HTTPS_PROXY / NO_PROXY in .env
  and also configure the Docker daemon proxy settings."
      warn "Pull failed — retrying in 5s (attempt ${pull_attempt}/3)..."
      sleep 5
    done
  fi

  log "Starting: ${SERVICES_TO_START[*]}"
  compose up -d "${SERVICES_TO_START[@]}"

  wait_for_kafka 180
  ensure_kafka_topics "${KAFKA_TOPICS[@]}"

  _show_next_steps
}

_show_next_steps() {
  local endpoints=""
  endpoints+="  PostgreSQL   localhost:5432\n"
  endpoints+="  Kafka        localhost:9092\n"
  (( START_AIRFLOW )) && endpoints+="  Airflow UI   http://127.0.0.1:8080  (admin / admin)\n"

  printf "\n${_CLR_BOLD}${_CLR_GREEN}Services are up.${_CLR_RESET}\n\n"
  printf "${_CLR_BOLD}Endpoints:${_CLR_RESET}\n%b\n" "$endpoints"
  printf "${_CLR_BOLD}Next steps:${_CLR_RESET}\n"
  cat <<'EOF'
  # 1. Export Python path and load environment
  export PYTHONPATH=src
  set -a && source .env && set +a

  # 2. Bootstrap the analytics warehouse (first time only)
  python -m medwarehouse warehouse bootstrap

  # 3. Run a domain pipeline end-to-end
  ./scripts/run_inventory_flow.sh     # inventory
  ./scripts/run_procurement_flow.sh   # procurement
  ./scripts/run_sales_flow.sh         # sales

  # 4. Start the monitoring platform
  python -m medwarehouse platform serve --host 127.0.0.1 --port 8787

  # 5. Start the operator webapp
  python -m medwarehouse_webapp --host 127.0.0.1 --port 8080

  # 6. Or run all three pipelines via Airflow
  ./scripts/start_services.sh --with-airflow

  # See docs/runbook.md for full operational guidance.
EOF
}

main "$@"
