-- ============================================================
-- SALES DOMAIN — Warehouse Layer
-- Event types: SALE_CREATED, SALE_CANCELLED
-- ============================================================

-- Customer dimension (SCD Type 1 — customer attributes rarely change analytically)
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_sk     BIGSERIAL PRIMARY KEY,
    customer_id     TEXT NOT NULL UNIQUE,
    full_name       TEXT,
    customer_type   TEXT,    -- RETAIL, HOSPITAL, DISTRIBUTOR
    created_at      TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON analytics.dim_customer (customer_id);


CREATE TABLE IF NOT EXISTS analytics.stg_sales_events (
    event_id                TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    event_time              TIMESTAMPTZ NOT NULL,
    event_date              DATE NOT NULL,
    producer                TEXT NOT NULL,
    schema_version          INTEGER NOT NULL,
    -- SALE_CREATED fields
    sale_id                 TEXT,
    product_id              TEXT,
    warehouse_id            TEXT,
    quantity                INTEGER,
    unit_price              NUMERIC(10,2),
    currency                TEXT,
    customer_type           TEXT,
    -- SALE_CANCELLED fields (reuses sale_id, product_id, warehouse_id, quantity)
    original_event_id       TEXT,
    -- Kafka metadata
    kafka_key               TEXT,
    source_topic            TEXT NOT NULL,
    source_partition        INTEGER NOT NULL,
    source_offset           BIGINT NOT NULL,
    source_kafka_timestamp  TIMESTAMPTZ,
    raw_event               TEXT NOT NULL,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);

CREATE INDEX IF NOT EXISTS idx_stg_sales_event_id   ON analytics.stg_sales_events (event_id);
CREATE INDEX IF NOT EXISTS idx_stg_sales_event_time ON analytics.stg_sales_events (event_time);
CREATE INDEX IF NOT EXISTS idx_stg_sales_sale_id    ON analytics.stg_sales_events (sale_id);


CREATE TABLE IF NOT EXISTS analytics.fact_sales_events (
    fact_sales_event_sk     BIGSERIAL,
    event_id                TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    event_time              TIMESTAMPTZ NOT NULL,
    event_date              DATE NOT NULL,
    producer                TEXT NOT NULL,
    schema_version          INTEGER NOT NULL,
    sale_id                 TEXT,
    product_id              TEXT,
    warehouse_id            TEXT,
    quantity                INTEGER,
    unit_price              NUMERIC(10,2),
    line_revenue            NUMERIC(12,2),   -- quantity * unit_price, negative for cancellations
    currency                TEXT,
    customer_type           TEXT,
    original_event_id       TEXT,
    -- Surrogate keys
    product_sk              BIGINT,
    warehouse_sk            BIGINT,
    -- Kafka metadata
    kafka_key               TEXT,
    source_topic            TEXT NOT NULL,
    source_partition        INTEGER NOT NULL,
    source_offset           BIGINT NOT NULL,
    source_kafka_timestamp  TIMESTAMPTZ,
    raw_event               TEXT NOT NULL,
    inserted_at             TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
) PARTITION BY RANGE (event_time);

CREATE TABLE IF NOT EXISTS analytics.fact_sales_events_default
    PARTITION OF analytics.fact_sales_events DEFAULT;

CREATE INDEX IF NOT EXISTS idx_fact_sales_event_id   ON analytics.fact_sales_events (event_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_event_date ON analytics.fact_sales_events (event_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_sale_id    ON analytics.fact_sales_events (sale_id);


CREATE OR REPLACE FUNCTION analytics.ensure_sales_fact_partition(p_month_start DATE)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    partition_name  TEXT;
    partition_start TIMESTAMPTZ;
    partition_end   TIMESTAMPTZ;
BEGIN
    partition_start := date_trunc('month', p_month_start)::timestamptz;
    partition_end   := (partition_start + interval '1 month')::timestamptz;
    partition_name  := format('fact_sales_events_%s', to_char(partition_start, 'YYYYMM'));
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS analytics.%I
         PARTITION OF analytics.fact_sales_events
         FOR VALUES FROM (%L) TO (%L)',
        partition_name, partition_start, partition_end
    );
END;
$$;

CREATE OR REPLACE FUNCTION analytics.ensure_sales_fact_partitions(p_from TIMESTAMPTZ, p_to TIMESTAMPTZ)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    cursor_month DATE;
    end_month    DATE;
BEGIN
    IF p_from IS NULL OR p_to IS NULL THEN RETURN; END IF;
    cursor_month := date_trunc('month', p_from)::date;
    end_month    := date_trunc('month', p_to)::date;
    WHILE cursor_month <= end_month LOOP
        PERFORM analytics.ensure_sales_fact_partition(cursor_month);
        cursor_month := (cursor_month + interval '1 month')::date;
    END LOOP;
END;
$$;


CREATE OR REPLACE FUNCTION analytics.refresh_dim_customer() RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    affected BIGINT := 0;
    row_count BIGINT;
BEGIN
    -- Upsert customers seen in sales events (customer_type is the only dim attribute here)
    INSERT INTO analytics.dim_customer (customer_id, full_name, customer_type, updated_at)
    SELECT DISTINCT
        s.customer_type AS customer_id,   -- customer_type is the grouping key for this lite dimension
        s.customer_type AS full_name,
        s.customer_type,
        timezone('UTC', now())
    FROM analytics.stg_sales_events s
    WHERE s.customer_type IS NOT NULL
    ON CONFLICT (customer_id) DO UPDATE
        SET updated_at = EXCLUDED.updated_at;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    affected := row_count;
    RETURN affected;
END;
$$;


CREATE OR REPLACE FUNCTION analytics.load_sales_event_facts() RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    inserted_count  BIGINT := 0;
    stage_min       TIMESTAMPTZ;
    stage_max       TIMESTAMPTZ;
BEGIN
    SELECT min(event_time), max(event_time)
    INTO stage_min, stage_max
    FROM analytics.stg_sales_events;

    PERFORM analytics.ensure_sales_fact_partitions(stage_min, stage_max);

    INSERT INTO analytics.fact_sales_events (
        event_id, event_type, event_time, event_date, producer, schema_version,
        sale_id, product_id, warehouse_id, quantity, unit_price, line_revenue,
        currency, customer_type, original_event_id,
        product_sk, warehouse_sk,
        kafka_key, source_topic, source_partition, source_offset,
        source_kafka_timestamp, raw_event, inserted_at
    )
    SELECT
        s.event_id, s.event_type, s.event_time, s.event_date, s.producer, s.schema_version,
        s.sale_id, s.product_id, s.warehouse_id, s.quantity, s.unit_price,
        CASE
            WHEN s.event_type = 'SALE_CREATED'  THEN  s.quantity * s.unit_price
            WHEN s.event_type = 'SALE_CANCELLED' THEN -s.quantity * s.unit_price
            ELSE 0
        END AS line_revenue,
        s.currency, s.customer_type, s.original_event_id,
        dp.product_sk, dw.warehouse_sk,
        s.kafka_key, s.source_topic, s.source_partition, s.source_offset,
        s.source_kafka_timestamp, s.raw_event, timezone('UTC', now())
    FROM analytics.stg_sales_events s
    LEFT JOIN analytics.dim_product   dp ON dp.product_id  = s.product_id  AND dp.is_current = TRUE
    LEFT JOIN analytics.dim_warehouse dw ON dw.warehouse_id= s.warehouse_id AND dw.is_current = TRUE
    WHERE NOT EXISTS (
        SELECT 1 FROM analytics.fact_sales_events f WHERE f.event_id = s.event_id
    );

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$;


CREATE OR REPLACE FUNCTION analytics.refresh_sales_semantic_layer() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    -- Daily revenue summary: net revenue after cancellations
    EXECUTE $v$
        CREATE OR REPLACE VIEW analytics.v_revenue_summary AS
        SELECT
            f.event_date,
            f.product_id,
            dp.brand_name,
            dp.generic_name,
            dp.form_factor,
            dp.hsn_code,
            f.warehouse_id,
            dw.warehouse_name,
            f.currency,
            f.customer_type,
            COUNT(*)    FILTER (WHERE f.event_type = 'SALE_CREATED')   AS transactions,
            SUM(f.quantity) FILTER (WHERE f.event_type = 'SALE_CREATED')    AS units_sold,
            SUM(f.line_revenue) FILTER (WHERE f.event_type = 'SALE_CREATED') AS gross_revenue,
            COUNT(*)    FILTER (WHERE f.event_type = 'SALE_CANCELLED') AS cancellations,
            SUM(f.quantity) FILTER (WHERE f.event_type = 'SALE_CANCELLED')   AS units_returned,
            SUM(f.line_revenue)                                          AS net_revenue
        FROM analytics.fact_sales_events f
        LEFT JOIN analytics.v_dim_product_current  dp ON dp.product_id  = f.product_id
        LEFT JOIN analytics.v_dim_warehouse_current dw ON dw.warehouse_id = f.warehouse_id
        WHERE f.product_id IS NOT NULL
        GROUP BY f.event_date, f.product_id, dp.brand_name, dp.generic_name,
                 dp.form_factor, dp.hsn_code, f.warehouse_id, dw.warehouse_name,
                 f.currency, f.customer_type
    $v$;

    -- Sales velocity: 7-day and 30-day rolling units for demand forecasting
    EXECUTE $v$
        CREATE OR REPLACE VIEW analytics.v_sales_velocity AS
        SELECT
            f.event_date,
            f.product_id,
            dp.brand_name,
            dp.generic_name,
            f.warehouse_id,
            dw.warehouse_name,
            SUM(f.quantity) FILTER (WHERE f.event_type = 'SALE_CREATED') AS daily_units_sold,
            ROUND(AVG(
                SUM(f.quantity) FILTER (WHERE f.event_type = 'SALE_CREATED')
            ) OVER (
                PARTITION BY f.product_id, f.warehouse_id
                ORDER BY f.event_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2) AS rolling_7d_avg,
            ROUND(AVG(
                SUM(f.quantity) FILTER (WHERE f.event_type = 'SALE_CREATED')
            ) OVER (
                PARTITION BY f.product_id, f.warehouse_id
                ORDER BY f.event_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 2) AS rolling_30d_avg
        FROM analytics.fact_sales_events f
        LEFT JOIN analytics.v_dim_product_current  dp ON dp.product_id  = f.product_id
        LEFT JOIN analytics.v_dim_warehouse_current dw ON dw.warehouse_id = f.warehouse_id
        WHERE f.product_id IS NOT NULL
        GROUP BY f.event_date, f.product_id, dp.brand_name, dp.generic_name,
                 f.warehouse_id, dw.warehouse_name
    $v$;

    -- Returns analysis: cancellation rate per product
    EXECUTE $v$
        CREATE OR REPLACE VIEW analytics.v_returns_analysis AS
        SELECT
            product_id,
            dp.brand_name,
            dp.generic_name,
            warehouse_id,
            dw.warehouse_name,
            SUM(quantity) FILTER (WHERE event_type = 'SALE_CREATED')   AS total_units_sold,
            SUM(quantity) FILTER (WHERE event_type = 'SALE_CANCELLED')  AS total_units_returned,
            ROUND(
                100.0 * SUM(quantity) FILTER (WHERE event_type = 'SALE_CANCELLED')
                / NULLIF(SUM(quantity) FILTER (WHERE event_type = 'SALE_CREATED'), 0), 2
            ) AS return_rate_pct
        FROM analytics.fact_sales_events
        LEFT JOIN analytics.v_dim_product_current dp USING (product_id)
        LEFT JOIN analytics.v_dim_warehouse_current dw USING (warehouse_id)
        WHERE product_id IS NOT NULL
        GROUP BY product_id, dp.brand_name, dp.generic_name, warehouse_id, dw.warehouse_name
        ORDER BY return_rate_pct DESC NULLS LAST
    $v$;
END;
$$;


CREATE OR REPLACE FUNCTION analytics.run_sales_quality_checks()
RETURNS TABLE(issue_name TEXT, issue_count BIGINT)
LANGUAGE sql SECURITY DEFINER AS $$
    SELECT 'staging_null_sale_id'::text, count(*)::bigint
    FROM analytics.stg_sales_events WHERE sale_id IS NULL

    UNION ALL

    SELECT 'staging_duplicate_event_ids'::text, count(*)::bigint
    FROM (
        SELECT event_id FROM analytics.stg_sales_events
        GROUP BY event_id HAVING count(*) > 1
    ) d

    UNION ALL

    SELECT 'staged_events_not_loaded'::text, count(*)::bigint
    FROM analytics.stg_sales_events s
    WHERE NOT EXISTS (
        SELECT 1 FROM analytics.fact_sales_events f WHERE f.event_id = s.event_id
    )

    UNION ALL

    SELECT 'sale_created_negative_revenue'::text, count(*)::bigint
    FROM analytics.fact_sales_events
    WHERE event_type = 'SALE_CREATED' AND line_revenue < 0

    UNION ALL

    SELECT 'fact_rows_in_default_partition'::text, count(*)::bigint
    FROM analytics.fact_sales_events_default
$$;


SELECT analytics.refresh_sales_semantic_layer();


-- Grants
GRANT SELECT, INSERT, DELETE, TRUNCATE ON analytics.stg_sales_events   TO {{SPARK_WRITER_USER}};
GRANT SELECT, INSERT                   ON analytics.fact_sales_events   TO {{SPARK_WRITER_USER}};
GRANT SELECT, INSERT, UPDATE           ON analytics.dim_customer         TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.ensure_sales_fact_partition(DATE)                    TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.ensure_sales_fact_partitions(TIMESTAMPTZ, TIMESTAMPTZ) TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.refresh_dim_customer()                               TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.load_sales_event_facts()                             TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.refresh_sales_semantic_layer()                       TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.run_sales_quality_checks()                           TO {{ANALYTICS_READER_USER}};

GRANT SELECT ON analytics.dim_customer           TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_revenue_summary       TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_sales_velocity        TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_returns_analysis      TO {{ANALYTICS_READER_USER}};
