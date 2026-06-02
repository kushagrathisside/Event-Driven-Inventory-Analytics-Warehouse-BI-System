CREATE OR REPLACE FUNCTION analytics.load_inventory_event_facts() RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    inserted_count BIGINT := 0;
    stage_min TIMESTAMPTZ;
    stage_max TIMESTAMPTZ;
BEGIN
    SELECT min(event_time), max(event_time)
    INTO stage_min, stage_max
    FROM analytics.stg_inventory_events;

    PERFORM analytics.ensure_inventory_fact_partitions(stage_min, stage_max);

    INSERT INTO analytics.fact_inventory_events (
        event_id,
        event_type,
        event_time,
        event_date,
        producer,
        schema_version,
        product_id,
        warehouse_id,
        batch_number,
        expiry_date,
        quantity_delta,
        supplier_id,
        sale_id,
        adjustment_reason,
        product_sk,
        warehouse_sk,
        supplier_sk,
        kafka_key,
        source_topic,
        source_partition,
        source_offset,
        source_kafka_timestamp,
        raw_event,
        inserted_at
    )
    SELECT
        s.event_id,
        s.event_type,
        s.event_time,
        s.event_date,
        s.producer,
        s.schema_version,
        s.product_id,
        s.warehouse_id,
        s.batch_number,
        s.expiry_date,
        s.quantity_delta,
        s.supplier_id,
        s.sale_id,
        s.adjustment_reason,
        dp.product_sk,
        dw.warehouse_sk,
        ds.supplier_sk,
        s.kafka_key,
        s.source_topic,
        s.source_partition,
        s.source_offset,
        s.source_kafka_timestamp,
        s.raw_event,
        timezone('UTC', now())
    FROM analytics.stg_inventory_events s
    INNER JOIN analytics.dim_product dp
        ON dp.product_id = s.product_id
       AND dp.is_current = TRUE
    INNER JOIN analytics.dim_warehouse dw
        ON dw.warehouse_id = s.warehouse_id
       AND dw.is_current = TRUE
    LEFT JOIN analytics.dim_supplier ds
        ON ds.supplier_id = s.supplier_id
       AND ds.is_current = TRUE
    WHERE NOT EXISTS (
        SELECT 1
        FROM analytics.fact_inventory_events f
        WHERE f.event_id = s.event_id
    );

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$;

CREATE OR REPLACE FUNCTION analytics.refresh_inventory_semantic_layer() RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    EXECUTE $view$
        CREATE OR REPLACE VIEW analytics.v_dim_supplier_current AS
        SELECT
            supplier_sk,
            supplier_id,
            supplier_name,
            gstin
        FROM analytics.dim_supplier
        WHERE is_current = TRUE
    $view$;

    EXECUTE $view$
        CREATE OR REPLACE VIEW analytics.v_dim_product_current AS
        SELECT
            product_sk,
            product_id,
            supplier_id,
            brand_name,
            generic_name,
            form_factor,
            hsn_code
        FROM analytics.dim_product
        WHERE is_current = TRUE
    $view$;

    EXECUTE $view$
        CREATE OR REPLACE VIEW analytics.v_dim_warehouse_current AS
        SELECT
            warehouse_sk,
            warehouse_id,
            warehouse_name,
            temperature_range
        FROM analytics.dim_warehouse
        WHERE is_current = TRUE
    $view$;

    EXECUTE $view$
        CREATE OR REPLACE VIEW analytics.v_inventory_balance AS
        SELECT
            f.product_sk,
            f.warehouse_sk,
            f.supplier_sk,
            f.product_id,
            f.warehouse_id,
            f.supplier_id,
            f.batch_number,
            f.expiry_date,
            sum(f.quantity_delta) AS current_quantity,
            max(f.event_time) AS last_event_time
        FROM analytics.fact_inventory_events f
        GROUP BY
            f.product_sk,
            f.warehouse_sk,
            f.supplier_sk,
            f.product_id,
            f.warehouse_id,
            f.supplier_id,
            f.batch_number,
            f.expiry_date
    $view$;

    EXECUTE $view$
        CREATE OR REPLACE VIEW analytics.v_inventory_snapshot AS
        SELECT
            balance.product_sk,
            balance.warehouse_sk,
            balance.supplier_sk,
            balance.product_id,
            product.brand_name,
            product.generic_name,
            product.form_factor,
            product.hsn_code,
            COALESCE(balance.supplier_id, product.supplier_id) AS supplier_id,
            supplier.supplier_name,
            supplier.gstin,
            balance.warehouse_id,
            warehouse.warehouse_name,
            warehouse.temperature_range,
            balance.batch_number,
            balance.expiry_date,
            balance.current_quantity,
            balance.last_event_time
        FROM analytics.v_inventory_balance balance
        INNER JOIN analytics.v_dim_product_current product
            ON product.product_sk = balance.product_sk
        INNER JOIN analytics.v_dim_warehouse_current warehouse
            ON warehouse.warehouse_sk = balance.warehouse_sk
        LEFT JOIN analytics.v_dim_supplier_current supplier
            ON supplier.supplier_sk = balance.supplier_sk
        WHERE balance.current_quantity <> 0
    $view$;
END;
$$;

CREATE OR REPLACE FUNCTION analytics.run_inventory_quality_checks()
RETURNS TABLE(issue_name TEXT, issue_count BIGINT)
LANGUAGE sql
SECURITY DEFINER
AS $$
    SELECT 'staging_null_business_keys'::text, count(*)::bigint
    FROM analytics.stg_inventory_events
    WHERE product_id IS NULL OR warehouse_id IS NULL OR batch_number IS NULL

    UNION ALL

    SELECT 'staging_duplicate_event_ids'::text, count(*)::bigint
    FROM (
        SELECT event_id
        FROM analytics.stg_inventory_events
        GROUP BY event_id
        HAVING count(*) > 1
    ) duplicates

    UNION ALL

    SELECT 'staged_events_not_loaded'::text, count(*)::bigint
    FROM analytics.stg_inventory_events s
    WHERE NOT EXISTS (
        SELECT 1
        FROM analytics.fact_inventory_events f
        WHERE f.event_id = s.event_id
    )

    UNION ALL

    SELECT 'fact_null_dimension_keys'::text, count(*)::bigint
    FROM analytics.fact_inventory_events
    WHERE product_sk IS NULL OR warehouse_sk IS NULL

    UNION ALL

    SELECT 'fact_orphan_product_dimension'::text, count(*)::bigint
    FROM analytics.fact_inventory_events f
    LEFT JOIN analytics.dim_product d
      ON d.product_sk = f.product_sk
    WHERE d.product_sk IS NULL

    UNION ALL

    SELECT 'fact_orphan_warehouse_dimension'::text, count(*)::bigint
    FROM analytics.fact_inventory_events f
    LEFT JOIN analytics.dim_warehouse d
      ON d.warehouse_sk = f.warehouse_sk
    WHERE d.warehouse_sk IS NULL

    UNION ALL

    SELECT 'fact_rows_in_default_partition'::text, count(*)::bigint
    FROM analytics.fact_inventory_events_default
$$;

SELECT analytics.refresh_inventory_semantic_layer();

GRANT USAGE ON SCHEMA analytics TO {{SPARK_WRITER_USER}};
GRANT SELECT, INSERT, DELETE, TRUNCATE ON analytics.stg_inventory_events TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.refresh_inventory_dimensions() TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.load_inventory_event_facts() TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.refresh_inventory_semantic_layer() TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.run_inventory_quality_checks() TO {{SPARK_WRITER_USER}};

GRANT USAGE ON SCHEMA analytics TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_dim_supplier_current TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_dim_product_current TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_dim_warehouse_current TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_inventory_balance TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_inventory_snapshot TO {{ANALYTICS_READER_USER}};
GRANT EXECUTE ON FUNCTION analytics.run_inventory_quality_checks() TO {{ANALYTICS_READER_USER}};
