# Medical Warehouse

Event-driven inventory analytics warehouse for a medical/pharmaceutical operation.

## What changed

- `src/medwarehouse/` is now the canonical implementation.
- Kafka producers, Spark jobs, and warehouse operations all run through one CLI: `python -m medwarehouse`.
- The inventory warehouse model is now event-sourced:
  - Bronze stores immutable Kafka payloads.
  - Silver stores validated, deduplicated inventory events plus quarantine records.
  - PostgreSQL `analytics.fact_inventory_events` is the canonical warehouse fact.
  - BI reads from `analytics.v_inventory_balance` and `analytics.v_inventory_snapshot`.

## Quick start

1. Copy `.env.example` to `.env` and adjust values if needed.
2. Start local infrastructure with:

```bash
./scripts/start_services.sh
```

This starts the core services required for the inventory flow:

- PostgreSQL
- Zookeeper
- Kafka

It also creates the required Kafka topics.

If you want Airflow too:

```bash
./scripts/start_services.sh --with-airflow
```

If your network uses a proxy:

- set `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` in `.env`
- configure the Docker daemon or Docker Desktop proxy settings too

The repo-level proxy settings cover container build/runtime. Docker still needs its own proxy path to pull images like `confluentinc/cp-kafka:7.5.0`.

3. Export `PYTHONPATH=src`.
4. Bootstrap the analytics warehouse:

```bash
python -m medwarehouse --log-level INFO warehouse bootstrap
```

5. Produce deterministic local inventory events:

```bash
python -m medwarehouse spark inventory-bronze
```

Leave Bronze running in a dedicated terminal, then in a second terminal:

```bash
python -m medwarehouse produce inventory --max-events 10
```

6. Run the canonical pipeline:

```bash
python -m medwarehouse spark inventory-silver
python -m medwarehouse orchestration build-gold
```

7. Start the local control plane:

```bash
python -m medwarehouse platform serve --host 127.0.0.1 --port 8787
```

Open `http://127.0.0.1:8787` to monitor infrastructure, artifacts, warehouse readiness, roles, job coverage, and to trigger the current inventory-stage controls.

The control plane now includes simple `Start Services` and `Stop Services` buttons that call the same infra scripts used from the terminal.

See [docs/runbook.md](/home/kushagra/medical-warehouse/docs/runbook.md) for the full operator flow.
