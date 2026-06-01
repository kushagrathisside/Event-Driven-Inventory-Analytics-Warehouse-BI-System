# Runbook

## Local environment

```bash
cp .env.example .env
./scripts/start_services.sh
export PYTHONPATH=src
set -a && source .env && set +a
```

`start_services.sh` does the following:

- creates `.env` from `.env.example` if needed
- starts PostgreSQL, Zookeeper, and Kafka by default
- waits for Kafka to become ready
- creates the required Kafka topics:
  - `inventory_events`
  - `procurement_events`
  - `sales_events`

To start Airflow too:

```bash
./scripts/start_services.sh --with-airflow
```

If you are behind a proxy:

- set `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` in `.env`
- keep `NO_PROXY` including local services such as `localhost`, `127.0.0.1`, `postgres`, `kafka`, and `zookeeper`
- configure the Docker daemon or Docker Desktop proxy settings as well

The `.env` values are used by Compose and the Airflow image build, but Docker image pulls still depend on Docker itself being able to reach `docker.io`.

## Bootstrap analytics warehouse

```bash
python -m medwarehouse warehouse bootstrap
```

This creates:

- roles
- `master` and `analytics` schemas
- FDW access into `medwarehouse_master`
- SCD dimensions
- staging table
- append-only partitioned fact table
- semantic views and quality checks

## Canonical inventory flow

```bash
python -m medwarehouse spark inventory-bronze
```

Leave Bronze running in one shell. In a second shell:

```bash
python -m medwarehouse produce inventory --max-events 10
python -m medwarehouse spark inventory-silver
python -m medwarehouse orchestration build-gold
```

## Airflow

- DAG id: `inventory_gold_pipeline`
- Tasks:
  - validate Silver availability
  - refresh dimensions
  - stage inventory events
  - load event facts
  - refresh semantic views
  - run quality checks

## Control plane

```bash
python -m medwarehouse platform serve --host 127.0.0.1 --port 8787
```

The UI shows:

- infrastructure start/stop controls and per-service status
- current domain coverage
- configured roles and detected warehouse roles
- Bronze, Silver, and quarantine artifact status
- analytics warehouse reachability and row counts
- Airflow DAG presence
- start/stop controls for the currently implemented jobs

## Replay / backfill

- Bronze is append-only.
- Silver is deterministic and overwrite-based for the same Bronze input.
- Facts are append-only and deduplicated by `event_id` during load.
- Partitions are auto-created from the staged event-time range.

## Recovery

- If Bronze is corrupted, replay from Kafka.
- If Silver is wrong, rerun `spark inventory-silver`.
- If warehouse facts or views are stale, rerun `orchestration build-gold`.
- If FDW connectivity breaks, fix credentials and rerun `warehouse bootstrap`.
