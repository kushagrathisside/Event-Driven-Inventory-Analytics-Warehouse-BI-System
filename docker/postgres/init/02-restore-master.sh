#!/bin/bash
set -euo pipefail

if [[ -f /workspace/meddata_full_20250916_175921.dump ]]; then
  echo "Restoring medwarehouse_master from bundled dump"
  pg_restore \
    --verbose \
    --clean \
    --if-exists \
    --no-owner \
    --dbname=postgresql://postgres:postgres@localhost:5432/medwarehouse_master \
    /workspace/meddata_full_20250916_175921.dump
fi
