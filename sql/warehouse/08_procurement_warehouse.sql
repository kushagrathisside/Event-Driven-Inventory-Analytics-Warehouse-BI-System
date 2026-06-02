-- ============================================================
-- PROCUREMENT DOMAIN — Warehouse Layer
-- Event types: PO_CREATED, PO_APPROVED, PO_RECEIVED, PO_CANCELLED
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.stg_procurement_events (
    event_id                TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    event_time              TIMESTAMPTZ NOT NULL,
    event_date              DATE NOT NULL,
    producer                TEXT NOT NULL,
    schema_version          INTEGER NOT NULL,
    -- PO_CREATED fields
    po_id                   TEXT,
    product_id              TEXT,
    ordered_quantity        INTEGER,
    supplier_id             TEXT,
    trigger_reason          TEXT,
    -- PO_APPROVED fields
    approved_by             TEXT,
    -- PO_RECEIVED fields
    received_quantity       INTEGER,
    warehouse_id            TEXT,
    -- PO_CANCELLED fields
    reason                  TEXT,
    -- Kafka metadata
    kafka_key               TEXT,
    source_topic            TEXT NOT NULL,
    source_partition        INTEGER NOT NULL,
    source_offset           BIGINT NOT NULL,
    source_kafka_timestamp  TIMESTAMPTZ,
    raw_event               TEXT NOT NULL,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);

CREATE INDEX IF NOT EXISTS idx_stg_procurement_event_id
    ON analytics.stg_procurement_events (event_id);
CREATE INDEX IF NOT EXISTS idx_stg_procurement_event_time
    ON analytics.stg_procurement_events (event_time);
CREATE INDEX IF NOT EXISTS idx_stg_procurement_po_id
    ON analytics.stg_procurement_events (po_id);


CREATE TABLE IF NOT EXISTS analytics.fact_procurement_events (
    fact_procurement_event_sk   BIGSERIAL,
    event_id                    TEXT NOT NULL,
    event_type                  TEXT NOT NULL,
    event_time                  TIMESTAMPTZ NOT NULL,
    event_date                  DATE NOT NULL,
    producer                    TEXT NOT NULL,
    schema_version              INTEGER NOT NULL,
    po_id                       TEXT,
    product_id                  TEXT,
    ordered_quantity            INTEGER,
    supplier_id                 TEXT,
    trigger_reason              TEXT,
    approved_by                 TEXT,
    received_quantity           INTEGER,
    warehouse_id                TEXT,
    reason                      TEXT,
    -- Surrogate keys (nullable: not all event types reference all dims)
    product_sk                  BIGINT,
    supplier_sk                 BIGINT,
    warehouse_sk                BIGINT,
    -- Kafka metadata
    kafka_key                   TEXT,
    source_topic                TEXT NOT NULL,
    source_partition            INTEGER NOT NULL,
    source_offset               BIGINT NOT NULL,
    source_kafka_timestamp      TIMESTAMPTZ,
    raw_event                   TEXT NOT NULL,
    inserted_at                 TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
) PARTITION BY RANGE (event_time);

CREATE TABLE IF NOT EXISTS analytics.fact_procurement_events_default
    PARTITION OF analytics.fact_procurement_events DEFAULT;

CREATE INDEX IF NOT EXISTS idx_fact_procurement_event_id
    ON analytics.fact_procurement_events (event_id);
CREATE INDEX IF NOT EXISTS idx_fact_procurement_event_date
    ON analytics.fact_procurement_events (event_date);
CREATE INDEX IF NOT EXISTS idx_fact_procurement_po_id
    ON analytics.fact_procurement_events (po_id);


CREATE OR REPLACE FUNCTION analytics.ensure_procurement_fact_partition(p_month_start DATE)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    partition_name  TEXT;
    partition_start TIMESTAMPTZ;
    partition_end   TIMESTAMPTZ;
BEGIN
    partition_start := date_trunc('month', p_month_start)::timestamptz;
    partition_end   := (partition_start + interval '1 month')::timestamptz;
    partition_name  := format('fact_procurement_events_%s', to_char(partition_start, 'YYYYMM'));
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS analytics.%I
         PARTITION OF analytics.fact_procurement_events
         FOR VALUES FROM (%L) TO (%L)',
        partition_name, partition_start, partition_end
    );
END;
$$;

CREATE OR REPLACE FUNCTION analytics.ensure_procurement_fact_partitions(p_from TIMESTAMPTZ, p_to TIMESTAMPTZ)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    cursor_month DATE;
    end_month    DATE;
BEGIN
    IF p_from IS NULL OR p_to IS NULL THEN RETURN; END IF;
    cursor_month := date_trunc('month', p_from)::date;
    end_month    := date_trunc('month', p_to)::date;
    WHILE cursor_month <= end_month LOOP
        PERFORM analytics.ensure_procurement_fact_partition(cursor_month);
        cursor_month := (cursor_month + interval '1 month')::date;
    END LOOP;
END;
$$;


CREATE OR REPLACE FUNCTION analytics.load_procurement_event_facts() RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    inserted_count  BIGINT := 0;
    stage_min       TIMESTAMPTZ;
    stage_max       TIMESTAMPTZ;
BEGIN
    SELECT min(event_time), max(event_time)
    INTO stage_min, stage_max
    FROM analytics.stg_procurement_events;

    PERFORM analytics.ensure_procurement_fact_partitions(stage_min, stage_max);

    INSERT INTO analytics.fact_procurement_events (
        event_id, event_type, event_time, event_date, producer, schema_version,
        po_id, product_id, ordered_quantity, supplier_id, trigger_reason,
        approved_by, received_quantity, warehouse_id, reason,
        product_sk, supplier_sk, warehouse_sk,
        kafka_key, source_topic, source_partition, source_offset,
        source_kafka_timestamp, raw_event, inserted_at
    )
    SELECT
        s.event_id, s.event_type, s.event_time, s.event_date, s.producer, s.schema_version,
        s.po_id, s.product_id, s.ordered_quantity, s.supplier_id, s.trigger_reason,
        s.approved_by, s.received_quantity, s.warehouse_id, s.reason,
        dp.product_sk, ds.supplier_sk, dw.warehouse_sk,
        s.kafka_key, s.source_topic, s.source_partition, s.source_offset,
        s.source_kafka_timestamp, s.raw_event, timezone('UTC', now())
    FROM analytics.stg_procurement_events s
    LEFT JOIN analytics.dim_product   dp ON dp.product_id  = s.product_id  AND dp.is_current = TRUE
    LEFT JOIN analytics.dim_supplier  ds ON ds.supplier_id = s.supplier_id AND ds.is_current = TRUE
    LEFT JOIN analytics.dim_warehouse dw ON dw.warehouse_id= s.warehouse_id AND dw.is_current = TRUE
    WHERE NOT EXISTS (
        SELECT 1 FROM analytics.fact_procurement_events f WHERE f.event_id = s.event_id
    );

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$;


CREATE OR REPLACE FUNCTION analytics.refresh_procurement_semantic_layer() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    -- PO lifecycle: latest status per purchase order
    EXECUTE $v$
        CREATE OR REPLACE VIEW analytics.v_po_lifecycle AS
        WITH created AS (
            SELECT po_id, product_id, supplier_id, ordered_quantity, trigger_reason,
                   event_time AS created_at
            FROM analytics.fact_procurement_events
            WHERE event_type = 'PO_CREATED' AND po_id IS NOT NULL
        ),
        approved AS (
            SELECT po_id, approved_by, event_time AS approved_at
            FROM analytics.fact_procurement_events
            WHERE event_type = 'PO_APPROVED' AND po_id IS NOT NULL
        ),
        received AS (
            SELECT po_id, received_quantity, warehouse_id, event_time AS received_at
            FROM analytics.fact_procurement_events
            WHERE event_type = 'PO_RECEIVED' AND po_id IS NOT NULL
        ),
        cancelled AS (
            SELECT po_id, reason AS cancellation_reason, event_time AS cancelled_at
            FROM analytics.fact_procurement_events
            WHERE event_type = 'PO_CANCELLED' AND po_id IS NOT NULL
        )
        SELECT
            c.po_id,
            c.product_id,
            dp.brand_name,
            dp.generic_name,
            c.supplier_id,
            ds.supplier_name,
            c.ordered_quantity,
            r.received_quantity,
            r.warehouse_id,
            dw.warehouse_name,
            c.trigger_reason,
            a.approved_by,
            c.created_at,
            a.approved_at,
            r.received_at,
            ca.cancelled_at,
            ca.cancellation_reason,
            CASE
                WHEN ca.po_id IS NOT NULL THEN 'CANCELLED'
                WHEN r.po_id  IS NOT NULL THEN 'RECEIVED'
                WHEN a.po_id  IS NOT NULL THEN 'APPROVED'
                ELSE 'PENDING'
            END AS po_status,
            CASE WHEN r.received_at IS NOT NULL
                THEN ROUND(EXTRACT(EPOCH FROM (r.received_at - c.created_at))/86400, 1)
            END AS actual_lead_time_days
        FROM created c
        LEFT JOIN approved  a  ON a.po_id  = c.po_id
        LEFT JOIN received  r  ON r.po_id  = c.po_id
        LEFT JOIN cancelled ca ON ca.po_id = c.po_id
        LEFT JOIN analytics.v_dim_product_current  dp ON dp.product_id  = c.product_id
        LEFT JOIN analytics.v_dim_supplier_current ds ON ds.supplier_id = c.supplier_id
        LEFT JOIN analytics.v_dim_warehouse_current dw ON dw.warehouse_id = r.warehouse_id
    $v$;

    -- Supplier performance scorecard
    EXECUTE $v$
        CREATE OR REPLACE VIEW analytics.v_supplier_performance AS
        SELECT
            po.supplier_id,
            ds.supplier_name,
            ds.supplier_id AS gstin_supplier,
            COUNT(DISTINCT po.po_id)                                                 AS total_pos,
            COUNT(DISTINCT po.po_id) FILTER (WHERE po.po_status = 'RECEIVED')        AS fulfilled_pos,
            COUNT(DISTINCT po.po_id) FILTER (WHERE po.po_status = 'CANCELLED')       AS cancelled_pos,
            COUNT(DISTINCT po.po_id) FILTER (WHERE po.po_status = 'PENDING')         AS open_pos,
            ROUND(
                100.0 * COUNT(DISTINCT po.po_id) FILTER (WHERE po.po_status = 'RECEIVED')
                / NULLIF(COUNT(DISTINCT po.po_id), 0), 1
            )                                                                        AS fulfillment_rate_pct,
            ROUND(AVG(po.actual_lead_time_days) FILTER (WHERE po.po_status = 'RECEIVED'), 1)
                                                                                     AS avg_lead_time_days,
            SUM(po.ordered_quantity)                                                 AS total_units_ordered,
            SUM(po.received_quantity)                                                AS total_units_received,
            ROUND(
                100.0 * SUM(po.received_quantity)
                / NULLIF(SUM(po.ordered_quantity), 0), 1
            )                                                                        AS fill_rate_pct
        FROM analytics.v_po_lifecycle po
        LEFT JOIN analytics.v_dim_supplier_current ds ON ds.supplier_id = po.supplier_id
        GROUP BY po.supplier_id, ds.supplier_name, ds.supplier_id
    $v$;

    -- Open PO aging (POs still pending beyond expected lead time)
    EXECUTE $v$
        CREATE OR REPLACE VIEW analytics.v_po_aging AS
        SELECT
            po.po_id,
            po.product_id,
            po.brand_name,
            po.supplier_id,
            po.supplier_name,
            po.ordered_quantity,
            po.created_at,
            po.approved_at,
            ROUND(EXTRACT(EPOCH FROM (timezone('UTC', now()) - po.created_at))/86400, 1) AS age_days,
            po.po_status
        FROM analytics.v_po_lifecycle po
        WHERE po.po_status IN ('PENDING', 'APPROVED')
        ORDER BY age_days DESC
    $v$;
END;
$$;


CREATE OR REPLACE FUNCTION analytics.run_procurement_quality_checks()
RETURNS TABLE(issue_name TEXT, issue_count BIGINT)
LANGUAGE sql SECURITY DEFINER AS $$
    SELECT 'staging_null_po_id'::text,
           count(*)::bigint
    FROM analytics.stg_procurement_events
    WHERE po_id IS NULL

    UNION ALL

    SELECT 'staging_duplicate_event_ids'::text,
           count(*)::bigint
    FROM (
        SELECT event_id FROM analytics.stg_procurement_events
        GROUP BY event_id HAVING count(*) > 1
    ) d

    UNION ALL

    SELECT 'staged_events_not_loaded'::text,
           count(*)::bigint
    FROM analytics.stg_procurement_events s
    WHERE NOT EXISTS (
        SELECT 1 FROM analytics.fact_procurement_events f WHERE f.event_id = s.event_id
    )

    UNION ALL

    SELECT 'fact_rows_in_default_partition'::text,
           count(*)::bigint
    FROM analytics.fact_procurement_events_default
$$;


-- Run semantic layer immediately so views exist after bootstrap
SELECT analytics.refresh_procurement_semantic_layer();


-- Grants
GRANT SELECT, INSERT, DELETE, TRUNCATE ON analytics.stg_procurement_events     TO {{SPARK_WRITER_USER}};
GRANT SELECT, INSERT                   ON analytics.fact_procurement_events     TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.ensure_procurement_fact_partition(DATE)            TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.ensure_procurement_fact_partitions(TIMESTAMPTZ, TIMESTAMPTZ) TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.load_procurement_event_facts()                     TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.refresh_procurement_semantic_layer()               TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.run_procurement_quality_checks()                   TO {{ANALYTICS_READER_USER}};

GRANT USAGE  ON SCHEMA analytics                               TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_po_lifecycle                       TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_supplier_performance               TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_po_aging                           TO {{ANALYTICS_READER_USER}};
