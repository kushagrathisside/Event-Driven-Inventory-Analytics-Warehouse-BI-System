#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE="$ROOT_DIR/.env"
ENV_EXAMPLE="$ROOT_DIR/.env.example"

CORE_SERVICES=(
  postgres
  zookeeper
  kafka
)

OPTIONAL_AIRFLOW_SERVICES=(
  airflow-init
  airflow-webserver
  airflow-scheduler
)

REQUIRED_TOPICS=(
  inventory_events
  procurement_events
  sales_events
)

log() {
  printf '[medwarehouse] %s\n' "$1"
}

fail() {
  printf '[medwarehouse] ERROR: %s\n' "$1" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

COMPOSE_CMD=()
SERVICES_TO_START=()
START_AIRFLOW=0

detect_compose() {
  local docker_compose_error=""
  local legacy_compose_error=""

  if command -v docker >/dev/null 2>&1; then
    if docker compose version >/dev/null 2>&1; then
      COMPOSE_CMD=(docker compose)
      return 0
    fi
    docker_compose_error="$(docker compose version 2>&1 || true)"
  fi

  if command -v docker-compose >/dev/null 2>&1; then
    if docker-compose version >/dev/null 2>&1; then
      COMPOSE_CMD=(docker-compose)
      return 0
    fi
    legacy_compose_error="$(docker-compose version 2>&1 || true)"
  fi

  fail "No usable Docker Compose command found.
Tried:
  - docker compose
  - docker-compose

docker compose output:
${docker_compose_error:-not available}

docker-compose output:
${legacy_compose_error:-not available}"
}

compose() {
  "${COMPOSE_CMD[@]}" "$@"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --with-airflow|--all)
        START_AIRFLOW=1
        shift
        ;;
      --core)
        START_AIRFLOW=0
        shift
        ;;
      *)
        fail "Unknown option: $1
Supported options:
  --core
  --with-airflow"
        ;;
    esac
  done

  SERVICES_TO_START=("${CORE_SERVICES[@]}")
  if [[ "$START_AIRFLOW" -eq 1 ]]; then
    SERVICES_TO_START+=("${OPTIONAL_AIRFLOW_SERVICES[@]}")
  fi
}

ensure_env_file() {
  if [[ ! -f "$ENV_FILE" ]]; then
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    log "Created .env from .env.example"
  fi
}

wait_for_kafka() {
  local retries=30
  local delay=2

  log "Waiting for Kafka to accept connections..."
  for ((i=1; i<=retries; i++)); do
    if compose exec -T kafka kafka-topics \
      --bootstrap-server kafka:29092 \
      --list >/dev/null 2>&1; then
      log "Kafka is ready"
      return 0
    fi
    sleep "$delay"
  done

  fail "Kafka did not become ready in time"
}

create_topics() {
  for topic in "${REQUIRED_TOPICS[@]}"; do
    log "Ensuring Kafka topic exists: $topic"
    compose exec -T kafka kafka-topics \
      --bootstrap-server kafka:29092 \
      --create \
      --if-not-exists \
      --topic "$topic" \
      --partitions 1 \
      --replication-factor 1 >/dev/null
  done
}

pull_with_retries() {
  local retries=3
  local delay=5
  local attempt=1

  while (( attempt <= retries )); do
    log "Pulling images (attempt ${attempt}/${retries})..."
    if compose pull "${SERVICES_TO_START[@]}"; then
      return 0
    fi

    if (( attempt == retries )); then
      fail "Image pull failed after ${retries} attempts.
This often happens because of a transient Docker registry/network issue such as a TLS handshake timeout.
If you are behind a proxy:
  1. Set HTTP_PROXY, HTTPS_PROXY, and NO_PROXY in .env
  2. Configure the Docker daemon or Docker Desktop proxy settings as well

Repo-level proxy settings help container builds and runtime, but Docker itself still needs direct proxy access to pull images from docker.io.
Please retry in a few minutes, or verify Docker Desktop / internet connectivity."
    fi

    log "Pull failed. Retrying in ${delay}s..."
    sleep "$delay"
    attempt=$((attempt + 1))
  done
}

show_next_steps() {
  cat <<'EOF'

[medwarehouse] Services are up.

Next steps:
  1. export PYTHONPATH=src
  2. set -a && source .env && set +a
  3. python -m medwarehouse warehouse bootstrap
  4. python -m medwarehouse spark inventory-bronze
  5. In another terminal:
     python -m medwarehouse produce inventory --max-events 10
     python -m medwarehouse spark inventory-silver
     python -m medwarehouse orchestration build-gold
  6. Optional control plane:
     python -m medwarehouse platform serve --host 127.0.0.1 --port 8787
  7. Optional Airflow stack:
     ./scripts/start_services.sh --with-airflow

Service endpoints:
  - PostgreSQL: localhost:5432
  - Kafka: localhost:9092
  - Airflow: http://127.0.0.1:8080
EOF
}

main() {
  parse_args "$@"
  detect_compose

  ensure_env_file

  pull_with_retries

  log "Starting services: ${SERVICES_TO_START[*]}"
  compose up -d "${SERVICES_TO_START[@]}"

  wait_for_kafka
  create_topics
  show_next_steps
}

main "$@"
