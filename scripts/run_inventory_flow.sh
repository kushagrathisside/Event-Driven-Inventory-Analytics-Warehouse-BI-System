#!/bin/bash
set -euo pipefail

export PYTHONPATH="${PYTHONPATH:-src}"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

python -m medwarehouse warehouse bootstrap
echo "Start 'python -m medwarehouse spark inventory-bronze' in a separate terminal before producing events."
python -m medwarehouse produce inventory --max-events "${1:-10}"
python -m medwarehouse spark inventory-silver
python -m medwarehouse orchestration build-gold
