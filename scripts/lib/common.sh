#!/usr/bin/env bash
# scripts/lib/common.sh — shared utilities for all medwarehouse scripts.
# Source this file; do not execute it directly.

# ── Terminal colours ──────────────────────────────────────────────────────────
if [[ -t 1 ]]; then
  _CLR_RESET='\033[0m'
  _CLR_BOLD='\033[1m'
  _CLR_GREEN='\033[0;32m'
  _CLR_YELLOW='\033[0;33m'
  _CLR_RED='\033[0;31m'
  _CLR_CYAN='\033[0;36m'
else
  _CLR_RESET='' _CLR_BOLD='' _CLR_GREEN='' _CLR_YELLOW='' _CLR_RED='' _CLR_CYAN=''
fi

# ── Logging ───────────────────────────────────────────────────────────────────
log()  { printf "${_CLR_CYAN}[mw]${_CLR_RESET} %s\n" "$1"; }
ok()   { printf "${_CLR_GREEN}[mw] ✓${_CLR_RESET} %s\n" "$1"; }
warn() { printf "${_CLR_YELLOW}[mw] ⚠${_CLR_RESET}  %s\n" "$1" >&2; }
fail() { printf "${_CLR_RED}[mw] ✗${_CLR_RESET}  %s\n" "$1" >&2; exit 1; }

header() {
  printf "\n${_CLR_BOLD}${_CLR_CYAN}══ %s ══${_CLR_RESET}\n\n" "$1"
}

# ── Project root (always the repo root, regardless of CWD) ───────────────────
# Scripts source this file; BASH_SOURCE[0] is common.sh itself.
# BASH_SOURCE[1] is the calling script.  Walk two levels up from lib/.
_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MW_ROOT="$(cd "${_COMMON_DIR}/../.." && pwd)"
export MW_ROOT

# ── Environment loading ───────────────────────────────────────────────────────
load_env() {
  local env_file="${MW_ROOT}/.env"
  local example_file="${MW_ROOT}/.env.example"

  if [[ ! -f "$env_file" ]]; then
    if [[ -f "$example_file" ]]; then
      cp "$example_file" "$env_file"
      warn ".env not found — copied from .env.example.  Review and adjust before production use."
    else
      warn ".env not found and no .env.example to copy from.  Using environment variables only."
      return 0
    fi
  fi

  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
}

# ── PYTHONPATH ────────────────────────────────────────────────────────────────
set_pythonpath() {
  local src_dir="${MW_ROOT}/src"
  if [[ ":${PYTHONPATH}:" != *":${src_dir}:"* ]]; then
    export PYTHONPATH="${src_dir}${PYTHONPATH:+:${PYTHONPATH}}"
  fi
}

# ── Prerequisite checks ───────────────────────────────────────────────────────
require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

require_python() {
  require_cmd python
  python --version >/dev/null 2>&1 || fail "python is not executable."
}

# ── Docker Compose detection (canonical, shared by start and stop) ────────────
COMPOSE_CMD=()

detect_compose() {
  for candidate in "docker compose" "docker-compose"; do
    # Split the candidate into an array
    read -ra parts <<< "$candidate"
    if command -v "${parts[0]}" >/dev/null 2>&1; then
      if "${parts[@]}" version >/dev/null 2>&1; then
        COMPOSE_CMD=("${parts[@]}")
        return 0
      fi
    fi
  done

  fail "No usable Docker Compose found.
  Tried: 'docker compose' and 'docker-compose'.

  Possible causes:
    • Docker Desktop is not running
    • docker CLI plugin is not installed
    • PATH does not include the Docker binary

  Fix:  https://docs.docker.com/compose/install/"
}

compose() {
  "${COMPOSE_CMD[@]}" "$@"
}

# ── Kafka readiness ───────────────────────────────────────────────────────────
# Exponential backoff: 1s, 2s, 4s, 8s, ... capped at 15s, up to max_wait seconds total.
wait_for_kafka() {
  local max_wait="${1:-180}"
  local elapsed=0
  local delay=1

  log "Waiting for Kafka to accept connections (up to ${max_wait}s)..."
  while (( elapsed < max_wait )); do
    if compose exec -T kafka kafka-topics \
        --bootstrap-server kafka:29092 \
        --list >/dev/null 2>&1; then
      ok "Kafka is ready (${elapsed}s)"
      return 0
    fi
    sleep "$delay"
    elapsed=$(( elapsed + delay ))
    delay=$(( delay < 15 ? delay * 2 : 15 ))
  done

  fail "Kafka did not become ready within ${max_wait}s.
  Check: $(compose) logs kafka | tail -30"
}

# ── Kafka topic creation ──────────────────────────────────────────────────────
ensure_kafka_topics() {
  local topics=("$@")
  for topic in "${topics[@]}"; do
    log "Ensuring topic: ${topic}"
    compose exec -T kafka kafka-topics \
      --bootstrap-server kafka:29092 \
      --create \
      --if-not-exists \
      --topic "$topic" \
      --partitions 1 \
      --replication-factor 1 >/dev/null \
      || fail "Failed to create Kafka topic: ${topic}"
  done
}

# ── Parquet output readiness ──────────────────────────────────────────────────
# Blocks until at least one .parquet file appears under the given directory.
wait_for_parquet() {
  local dir="$1"
  local max_wait="${2:-60}"
  local elapsed=0
  local delay=1

  log "Waiting for parquet output in: ${dir}"
  while (( elapsed < max_wait )); do
    if find "$dir" -name "*.parquet" -print -quit 2>/dev/null | grep -q .; then
      ok "Parquet data is available (${elapsed}s)"
      return 0
    fi
    sleep "$delay"
    elapsed=$(( elapsed + delay ))
  done

  fail "No parquet files found in '${dir}' after ${max_wait}s.
  The Bronze stream may not be running or Kafka may not have received events.
  Start: python -m medwarehouse spark $(basename "$(dirname "$dir")")-bronze"
}
