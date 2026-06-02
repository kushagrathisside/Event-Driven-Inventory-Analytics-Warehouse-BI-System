-- Physical inventory snapshot: one row per product+warehouse+batch.
-- Refreshed by analytics.refresh_inventory_balance() after each Gold run.
-- Replaces the derived v_inventory_balance view for Power BI performance.
CREATE TABLE IF NOT EXISTS analytics.fact_inventory_balance (
    product_id          TEXT NOT NULL,
    warehouse_id        TEXT NOT NULL,
    batch_number        TEXT NOT NULL,
    expiry_date         DATE,
    current_quantity    INTEGER NOT NULL,
    last_event_time     TIMESTAMPTZ NOT NULL,
    product_sk          BIGINT NOT NULL,
    warehouse_sk        BIGINT NOT NULL,
    supplier_sk         BIGINT,
    refreshed_at        TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    CONSTRAINT pk_fact_inventory_balance PRIMARY KEY (product_id, warehouse_id, batch_number)
);

CREATE INDEX IF NOT EXISTS idx_fact_inv_balance_product
    ON analytics.fact_inventory_balance (product_id);

CREATE INDEX IF NOT EXISTS idx_fact_inv_balance_warehouse
    ON analytics.fact_inventory_balance (warehouse_id);

CREATE INDEX IF NOT EXISTS idx_fact_inv_balance_qty
    ON analytics.fact_inventory_balance (current_quantity)
    WHERE current_quantity > 0;


-- Operator-configured reorder thresholds per product+warehouse pair.
CREATE TABLE IF NOT EXISTS analytics.reorder_policies (
    id                      BIGSERIAL PRIMARY KEY,
    product_id              TEXT NOT NULL,
    warehouse_id            TEXT NOT NULL,
    reorder_point           INTEGER NOT NULL CHECK (reorder_point >= 0),
    reorder_quantity        INTEGER NOT NULL CHECK (reorder_quantity > 0),
    preferred_supplier_id   TEXT,
    lead_time_days          INTEGER NOT NULL DEFAULT 7 CHECK (lead_time_days > 0),
    active                  BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);

-- Only one active policy per product+warehouse at a time.
CREATE UNIQUE INDEX IF NOT EXISTS uix_reorder_policies_active
    ON analytics.reorder_policies (product_id, warehouse_id)
    WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_reorder_policies_product
    ON analytics.reorder_policies (product_id, active);


-- Auto-generated purchase orders created by check_reorder_thresholds().
-- Operator reviews, approves, and sends from the webapp.
CREATE TABLE IF NOT EXISTS analytics.pending_purchase_orders (
    po_id               TEXT PRIMARY KEY,
    product_id          TEXT NOT NULL,
    warehouse_id        TEXT NOT NULL,
    supplier_id         TEXT NOT NULL,
    ordered_quantity    INTEGER NOT NULL CHECK (ordered_quantity > 0),
    trigger_reason      TEXT NOT NULL DEFAULT 'AUTO_REORDER',
    status              TEXT NOT NULL DEFAULT 'DRAFT'
                            CHECK (status IN ('DRAFT', 'APPROVED', 'SENT', 'RECEIVED', 'CANCELLED')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    approved_at         TIMESTAMPTZ,
    sent_at             TIMESTAMPTZ,
    received_at         TIMESTAMPTZ,
    approved_by         TEXT,
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_pending_po_status
    ON analytics.pending_purchase_orders (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pending_po_product_warehouse
    ON analytics.pending_purchase_orders (product_id, warehouse_id, status);


-- Grants for existing roles
GRANT SELECT, INSERT, UPDATE ON analytics.fact_inventory_balance TO {{SPARK_WRITER_USER}};
GRANT SELECT, INSERT, UPDATE ON analytics.reorder_policies       TO {{SPARK_WRITER_USER}};
GRANT SELECT, INSERT, UPDATE ON analytics.pending_purchase_orders TO {{SPARK_WRITER_USER}};

GRANT SELECT ON analytics.fact_inventory_balance      TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.reorder_policies            TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.pending_purchase_orders     TO {{ANALYTICS_READER_USER}};

GRANT USAGE ON SEQUENCE analytics.reorder_policies_id_seq TO {{SPARK_WRITER_USER}};
