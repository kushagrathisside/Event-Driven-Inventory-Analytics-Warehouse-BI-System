# Target Architecture

## HLD

The codebase is organized around five layers:

1. **Source**
   - `medwarehouse_master` remains the operational source.
   - `medwarehouse_analytics` hosts the analytical warehouse.
   - PostgreSQL FDW exposes selected operational tables into the `master` schema inside the analytics database.

2. **Ingestion**
   - Kafka topics carry immutable business events keyed by business identifiers.
   - Producers emit deterministic local sample data for repeatable development runs.

3. **Processing**
   - Bronze stores raw Kafka payloads and metadata append-only.
   - Silver parses, validates, deduplicates, and quarantines invalid records.
   - Spark Gold does not publish BI-facing balance tables; it only stages warehouse-ready inventory events.

4. **Warehouse**
   - `analytics.dim_product`, `analytics.dim_supplier`, and `analytics.dim_warehouse` are SCD Type-2 dimensions.
   - `analytics.fact_inventory_events` is append-only and partitioned by `event_time`.
   - `analytics.v_inventory_balance` and `analytics.v_inventory_snapshot` are the public analytical interfaces.

5. **Consumption**
   - Airflow orchestrates the warehouse build.
   - Power BI reads only curated semantic views and current-dimension views.

## LLD

### Canonical package

- `medwarehouse.config` loads all runtime configuration from environment variables.
- `medwarehouse.cli` exposes one command surface for producers, Spark jobs, and warehouse operations.
- `medwarehouse.contracts.inventory` defines the shared event envelope and local deterministic sample generation.
- `medwarehouse.spark.jobs.*` contains the only canonical Spark implementations.
- `medwarehouse.warehouse.*` applies SQL, validates datasets, and executes warehouse load steps.

### Inventory data flow

1. `produce inventory`
2. `spark inventory-bronze`
3. `spark inventory-silver`
4. `spark stage-inventory-events`
5. `warehouse refresh-dimensions`
6. `warehouse load-facts`
7. `warehouse refresh-views`
8. `warehouse quality-checks`

### Security model

- `spark_writer`
  - direct write access only to warehouse staging tables
  - execute permission on curated warehouse procedures
- `analytics_reader`
  - read-only access to curated semantic views
- `fdw_reader`
  - FDW-backed access path for operational source ingestion inside the analytics database
