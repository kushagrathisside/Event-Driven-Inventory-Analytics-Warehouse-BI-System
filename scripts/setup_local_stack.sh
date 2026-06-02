#!/usr/bin/env bash
# scripts/setup_local_stack.sh — complete first-time local environment setup.
#
# Run this once on a fresh clone.  It will:
#   1.  Check prerequisites (Docker, Python 3.11+, Java 17)
#   2.  Create .env from .env.example
#   3.  Create a Python virtual environment and install dependencies
#   4.  Start Docker infrastructure (PostgreSQL + Kafka)
#   5.  Bootstrap the analytics warehouse (all SQL files)
#   6.  Print a summary of what to do next
#
# Usage:
#   ./scripts/setup_local_stack.sh [--no-venv] [--no-pull]
#
# Options:
#   --no-venv    Skip Python venv creation (use active venv or system Python)
#   --no-pull    Skip Docker image pull
#   --help       Show this message
set -euo pipefail

# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"

CREATE_VENV=1
PULL_IMAGES=1

for arg in "$@"; do
  case "$arg" in
    --no-venv)  CREATE_VENV=0  ;;
    --no-pull)  PULL_IMAGES=0  ;;
    --help|-h)  grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) fail "Unknown option: $arg  (run with --help for usage)" ;;
  esac
done

# ── Prerequisite checks ───────────────────────────────────────────────────────
check_prerequisites() {
  header "Checking Prerequisites"

  require_cmd docker

  # Python 3.11+
  require_cmd python
  local py_ver
  py_ver="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  local py_major py_minor
  py_major="${py_ver%%.*}"
  py_minor="${py_ver#*.}"
  if (( py_major < 3 || (py_major == 3 && py_minor < 11) )); then
    fail "Python 3.11 or newer is required.  Found: ${py_ver}"
  fi
  ok "Python ${py_ver}"

  # Java 17 (required by PySpark)
  if command -v java >/dev/null 2>&1; then
    local java_ver
    java_ver="$(java -version 2>&1 | head -1 | sed 's/.*version "\([0-9]*\).*/\1/')"
    if (( java_ver >= 17 )); then
      ok "Java ${java_ver}"
    else
      warn "Java ${java_ver} found but Java 17+ is required for PySpark.
  Install: sudo apt-get install -y openjdk-17-jre-headless
  Then: export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64"
    fi
  else
    warn "Java not found — PySpark will not work.
  Install: sudo apt-get install -y openjdk-17-jre-headless
  Then: export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64"
  fi
}

# ── Python virtual environment ────────────────────────────────────────────────
setup_venv() {
  header "Python Environment"
  local venv_dir="${MW_ROOT}/.venv"

  if [[ -d "$venv_dir" ]]; then
    ok "Virtual environment already exists at .venv/"
  else
    log "Creating virtual environment at .venv/ ..."
    python -m venv "$venv_dir"
    ok "Virtual environment created."
  fi

  # Activate for the duration of this script
  # shellcheck disable=SC1091
  source "${venv_dir}/bin/activate"

  log "Installing dependencies from requirements.txt (this may take a few minutes)..."
  pip install --quiet --upgrade pip
  pip install --quiet -r "${MW_ROOT}/requirements.txt"
  ok "Dependencies installed."
}

# ── Bootstrap the warehouse ───────────────────────────────────────────────────
bootstrap_warehouse() {
  header "Analytics Warehouse Bootstrap"
  log "Applying all SQL files (01–10)..."
  python -m medwarehouse warehouse bootstrap
  ok "Warehouse bootstrapped."
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  header "MedWarehouse — First-Time Setup"

  check_prerequisites
  load_env  # creates .env from example if missing

  if (( CREATE_VENV )); then
    setup_venv
  fi

  set_pythonpath

  # Start infrastructure (uses --no-pull flag if requested)
  local start_flags=()
  (( PULL_IMAGES )) || start_flags+=(--no-pull)
  "${MW_ROOT}/scripts/start_services.sh" "${start_flags[@]}"

  bootstrap_warehouse

  printf "\n${_CLR_BOLD}${_CLR_GREEN}Setup complete.${_CLR_RESET}\n\n"
  cat <<'EOF'
Quick start (copy and run in order):

  # Activate Python environment
  source .venv/bin/activate
  set -a && source .env && set +a

  # Run the inventory pipeline (Bronze → Silver → Gold)
  ./scripts/run_inventory_flow.sh 10

  # Run procurement and sales pipelines
  ./scripts/run_procurement_flow.sh 15
  ./scripts/run_sales_flow.sh 20

  # Start the monitoring dashboard
  python -m medwarehouse platform serve --host 127.0.0.1 --port 8787

  # Start the operator webapp
  python -m medwarehouse_webapp --host 127.0.0.1 --port 8080

  # Full documentation
  cat docs/runbook.md

EOF
}

main "$@"
