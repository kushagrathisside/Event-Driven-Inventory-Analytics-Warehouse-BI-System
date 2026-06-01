# Current-State Baseline

This baseline freezes the prototype state before the refactor.

## Observed drift points

- Two different Silver implementations existed with conflicting semantics.
- `inventory_clean.py` depended on `silver_df` implicitly and was not a standalone job.
- Hardcoded endpoints and credentials were embedded in producers and Spark jobs.
- Database targets were inconsistent across scripts: `medwarehouse_master`, `medwarehouse_analytics`, and `medwarehouse`.
- The old Gold path mixed:
  - parquet balance aggregation
  - direct JDBC writes into balance tables
  - a staging/merge path
- The repo contained a Spark swap file and generated local metastore state alongside source code.

## Prototype run evidence

- Bronze parquet exists under `data/bronze/inventory_events`.
- Silver parquet exists under `data/silver/inventory_events`.
- Gold parquet exists under `data/gold/inventory_balance` and `data/gold/inventory_balance_enriched`.
- Checkpoints exist under `data/checkpoints/inventory_events`.

## Regression target

The refactor preserves the useful end-to-end behavior:

1. produce inventory events
2. ingest immutable Bronze
3. normalize and quarantine in Silver
4. materialize a current inventory snapshot for BI

The canonical implementation now changes how step 4 works: the BI snapshot is derived from warehouse event facts and semantic views rather than direct balance parquet outputs.
