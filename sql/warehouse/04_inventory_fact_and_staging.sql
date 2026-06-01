CREATE TABLE IF NOT EXISTS analytics.stg_inventory_events (
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    producer TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    warehouse_id TEXT NOT NULL,
    batch_number TEXT NOT NULL,
    expiry_date DATE,
    quantity_delta INTEGER NOT NULL,
    supplier_id TEXT,
    sale_id TEXT,
    adjustment_reason TEXT,
    kafka_key TEXT,
    source_topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    source_kafka_timestamp TIMESTAMPTZ,
    raw_event TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);

CREATE INDEX IF NOT EXISTS idx_stg_inventory_events_event_id
    ON analytics.stg_inventory_events (event_id);

CREATE INDEX IF NOT EXISTS idx_stg_inventory_events_event_time
    ON analytics.stg_inventory_events (event_time);

CREATE TABLE IF NOT EXISTS analytics.fact_inventory_events (
    fact_inventory_event_sk BIGSERIAL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    producer TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    warehouse_id TEXT NOT NULL,
    batch_number TEXT NOT NULL,
    expiry_date DATE,
    quantity_delta INTEGER NOT NULL,
    supplier_id TEXT,
    sale_id TEXT,
    adjustment_reason TEXT,
    product_sk BIGINT NOT NULL,
    warehouse_sk BIGINT NOT NULL,
    supplier_sk BIGINT,
    kafka_key TEXT,
    source_topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    source_kafka_timestamp TIMESTAMPTZ,
    raw_event TEXT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
) PARTITION BY RANGE (event_time);

CREATE TABLE IF NOT EXISTS analytics.fact_inventory_events_default
    PARTITION OF analytics.fact_inventory_events DEFAULT;

CREATE INDEX IF NOT EXISTS idx_fact_inventory_events_event_id
    ON analytics.fact_inventory_events (event_id);

CREATE INDEX IF NOT EXISTS idx_fact_inventory_events_event_date
    ON analytics.fact_inventory_events (event_date);

CREATE OR REPLACE FUNCTION analytics.ensure_inventory_fact_partition(p_month_start DATE) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    partition_name TEXT;
    partition_start TIMESTAMPTZ;
    partition_end TIMESTAMPTZ;
BEGIN
    partition_start := date_trunc('month', p_month_start)::timestamptz;
    partition_end := (partition_start + interval '1 month')::timestamptz;
    partition_name := format('fact_inventory_events_%s', to_char(partition_start, 'YYYYMM'));

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS analytics.%I PARTITION OF analytics.fact_inventory_events
         FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        partition_start,
        partition_end
    );
END;
$$;

CREATE OR REPLACE FUNCTION analytics.ensure_inventory_fact_partitions(
    p_from TIMESTAMPTZ,
    p_to TIMESTAMPTZ
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    cursor_month DATE;
    end_month DATE;
BEGIN
    IF p_from IS NULL OR p_to IS NULL THEN
        RETURN;
    END IF;

    cursor_month := date_trunc('month', p_from)::date;
    end_month := date_trunc('month', p_to)::date;
    WHILE cursor_month <= end_month LOOP
        PERFORM analytics.ensure_inventory_fact_partition(cursor_month);
        cursor_month := (cursor_month + interval '1 month')::date;
    END LOOP;
END;
$$;
