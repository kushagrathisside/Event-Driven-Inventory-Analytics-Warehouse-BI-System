-- Merge v_inventory_balance into fact_inventory_balance.
-- Called after every Gold pipeline run to keep the physical snapshot current.
CREATE OR REPLACE FUNCTION analytics.refresh_inventory_balance() RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    rows_upserted BIGINT := 0;
    rows_deleted  BIGINT := 0;
BEGIN
    -- Upsert: insert new rows and update changed rows
    INSERT INTO analytics.fact_inventory_balance (
        product_id,
        warehouse_id,
        batch_number,
        expiry_date,
        current_quantity,
        last_event_time,
        product_sk,
        warehouse_sk,
        supplier_sk,
        refreshed_at
    )
    SELECT
        b.product_id,
        b.warehouse_id,
        b.batch_number,
        b.expiry_date,
        b.current_quantity,
        b.last_event_time,
        b.product_sk,
        b.warehouse_sk,
        b.supplier_sk,
        timezone('UTC', now())
    FROM analytics.v_inventory_balance b
    ON CONFLICT (product_id, warehouse_id, batch_number)
    DO UPDATE SET
        current_quantity = EXCLUDED.current_quantity,
        last_event_time  = EXCLUDED.last_event_time,
        expiry_date      = EXCLUDED.expiry_date,
        product_sk       = EXCLUDED.product_sk,
        warehouse_sk     = EXCLUDED.warehouse_sk,
        supplier_sk      = EXCLUDED.supplier_sk,
        refreshed_at     = EXCLUDED.refreshed_at;

    GET DIAGNOSTICS rows_upserted = ROW_COUNT;

    -- Remove rows for batches that no longer appear in the event fact
    -- (fully consumed or reversed to zero then further events removed them)
    DELETE FROM analytics.fact_inventory_balance f
    WHERE NOT EXISTS (
        SELECT 1
        FROM analytics.v_inventory_balance b
        WHERE b.product_id   = f.product_id
          AND b.warehouse_id = f.warehouse_id
          AND b.batch_number = f.batch_number
    );

    GET DIAGNOSTICS rows_deleted = ROW_COUNT;

    RETURN rows_upserted + rows_deleted;
END;
$$;


-- Check all active reorder policies against current balance snapshot.
-- Creates DRAFT purchase orders for any product+warehouse below threshold
-- where no open (non-CANCELLED, non-RECEIVED) PO already exists.
CREATE OR REPLACE FUNCTION analytics.check_reorder_thresholds() RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    orders_created BIGINT := 0;
BEGIN
    INSERT INTO analytics.pending_purchase_orders (
        po_id,
        product_id,
        warehouse_id,
        supplier_id,
        ordered_quantity,
        trigger_reason,
        status,
        created_at
    )
    SELECT
        -- Deterministic PO ID: same product+warehouse on the same calendar day
        -- produces the same UUID, preventing duplicate orders within a day.
        encode(
            digest(
                p.product_id || '|' || p.warehouse_id || '|' ||
                to_char(timezone('UTC', now()), 'YYYY-MM-DD'),
                'sha256'
            ),
            'hex'
        )::text AS po_id,
        p.product_id,
        p.warehouse_id,
        COALESCE(p.preferred_supplier_id, dp.supplier_id, 'UNKNOWN') AS supplier_id,
        p.reorder_quantity,
        'AUTO_REORDER',
        'DRAFT',
        timezone('UTC', now())
    FROM analytics.reorder_policies p
    JOIN analytics.fact_inventory_balance b
        ON b.product_id   = p.product_id
       AND b.warehouse_id = p.warehouse_id
    LEFT JOIN analytics.v_dim_product_current dp
        ON dp.product_id = p.product_id
    WHERE p.active = TRUE
      AND b.current_quantity < p.reorder_point
      AND NOT EXISTS (
          SELECT 1
          FROM analytics.pending_purchase_orders po
          WHERE po.product_id   = p.product_id
            AND po.warehouse_id = p.warehouse_id
            AND po.status NOT IN ('CANCELLED', 'RECEIVED')
      )
    ON CONFLICT (po_id) DO NOTHING;

    GET DIAGNOSTICS orders_created = ROW_COUNT;
    RETURN orders_created;
END;
$$;


-- View: products approaching or below their reorder threshold.
-- Used by Power BI risk dashboard and webapp operator dashboard.
CREATE OR REPLACE VIEW analytics.v_reorder_risk AS
SELECT
    b.product_id,
    b.warehouse_id,
    b.batch_number,
    b.expiry_date,
    b.current_quantity,
    p.reorder_point,
    p.reorder_quantity,
    p.lead_time_days,
    p.preferred_supplier_id,
    b.current_quantity - p.reorder_point   AS quantity_gap,
    CASE
        WHEN b.current_quantity <= 0              THEN 'OUT_OF_STOCK'
        WHEN b.current_quantity < p.reorder_point THEN 'BELOW_THRESHOLD'
        WHEN b.current_quantity < p.reorder_point * 1.25 THEN 'APPROACHING_THRESHOLD'
        ELSE 'HEALTHY'
    END AS risk_status,
    dp.brand_name,
    dp.generic_name,
    dw.warehouse_name
FROM analytics.fact_inventory_balance b
JOIN analytics.reorder_policies p
    ON p.product_id   = b.product_id
   AND p.warehouse_id = b.warehouse_id
   AND p.active = TRUE
LEFT JOIN analytics.v_dim_product_current dp
    ON dp.product_id = b.product_id
LEFT JOIN analytics.v_dim_warehouse_current dw
    ON dw.warehouse_id = b.warehouse_id;


-- View: purchase order list enriched with product and warehouse names.
-- Primary consumption surface for the webapp PO management page and Power BI.
CREATE OR REPLACE VIEW analytics.v_purchase_order_status AS
SELECT
    po.po_id,
    po.product_id,
    po.warehouse_id,
    po.supplier_id,
    po.ordered_quantity,
    po.trigger_reason,
    po.status,
    po.created_at,
    po.approved_at,
    po.sent_at,
    po.received_at,
    po.approved_by,
    po.notes,
    dp.brand_name,
    dp.generic_name,
    dw.warehouse_name,
    ds.supplier_name
FROM analytics.pending_purchase_orders po
LEFT JOIN analytics.v_dim_product_current dp
    ON dp.product_id = po.product_id
LEFT JOIN analytics.v_dim_warehouse_current dw
    ON dw.warehouse_id = po.warehouse_id
LEFT JOIN analytics.v_dim_supplier_current ds
    ON ds.supplier_id = po.supplier_id;


-- Grants
GRANT EXECUTE ON FUNCTION analytics.refresh_inventory_balance()   TO {{SPARK_WRITER_USER}};
GRANT EXECUTE ON FUNCTION analytics.check_reorder_thresholds()    TO {{SPARK_WRITER_USER}};

GRANT SELECT ON analytics.v_reorder_risk           TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_purchase_order_status  TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_reorder_risk           TO {{SPARK_WRITER_USER}};
GRANT SELECT ON analytics.v_purchase_order_status  TO {{SPARK_WRITER_USER}};
