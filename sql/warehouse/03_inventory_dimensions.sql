CREATE TABLE IF NOT EXISTS analytics.dim_supplier (
    supplier_sk BIGSERIAL PRIMARY KEY,
    supplier_id TEXT NOT NULL,
    supplier_name TEXT,
    gstin TEXT,
    valid_from TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);

CREATE INDEX IF NOT EXISTS idx_dim_supplier_current
    ON analytics.dim_supplier (supplier_id, is_current);

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_sk BIGSERIAL PRIMARY KEY,
    product_id TEXT NOT NULL,
    supplier_id TEXT,
    brand_name TEXT,
    generic_name TEXT,
    form_factor TEXT,
    hsn_code TEXT,
    valid_from TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);

CREATE INDEX IF NOT EXISTS idx_dim_product_current
    ON analytics.dim_product (product_id, is_current);

CREATE TABLE IF NOT EXISTS analytics.dim_warehouse (
    warehouse_sk BIGSERIAL PRIMARY KEY,
    warehouse_id TEXT NOT NULL,
    warehouse_name TEXT,
    temperature_range TEXT,
    valid_from TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);

CREATE INDEX IF NOT EXISTS idx_dim_warehouse_current
    ON analytics.dim_warehouse (warehouse_id, is_current);

CREATE OR REPLACE FUNCTION analytics.refresh_dim_supplier() RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    affected BIGINT := 0;
    row_count BIGINT := 0;
BEGIN
    UPDATE analytics.dim_supplier d
    SET valid_to = timezone('UTC', now()),
        is_current = FALSE,
        updated_at = timezone('UTC', now())
    WHERE d.is_current = TRUE
      AND NOT EXISTS (
          SELECT 1
          FROM master.suppliers s
          WHERE s.supplier_id::text = d.supplier_id
      );
    GET DIAGNOSTICS row_count = ROW_COUNT;
    affected := affected + row_count;

    UPDATE analytics.dim_supplier d
    SET valid_to = timezone('UTC', now()),
        is_current = FALSE,
        updated_at = timezone('UTC', now())
    FROM master.suppliers s
    WHERE d.supplier_id = s.supplier_id::text
      AND d.is_current = TRUE
      AND (
          COALESCE(d.supplier_name, '') <> COALESCE(s.name, '')
          OR COALESCE(d.gstin, '') <> COALESCE(s.gstin, '')
      );
    GET DIAGNOSTICS row_count = ROW_COUNT;
    affected := affected + row_count;

    INSERT INTO analytics.dim_supplier (
        supplier_id,
        supplier_name,
        gstin,
        valid_from,
        valid_to,
        is_current,
        created_at,
        updated_at
    )
    SELECT
        s.supplier_id::text,
        s.name,
        s.gstin,
        timezone('UTC', now()),
        NULL,
        TRUE,
        timezone('UTC', now()),
        timezone('UTC', now())
    FROM master.suppliers s
    LEFT JOIN analytics.dim_supplier d
      ON d.supplier_id = s.supplier_id::text
     AND d.is_current = TRUE
    WHERE d.supplier_id IS NULL
       OR COALESCE(d.supplier_name, '') <> COALESCE(s.name, '')
       OR COALESCE(d.gstin, '') <> COALESCE(s.gstin, '');
    GET DIAGNOSTICS row_count = ROW_COUNT;
    affected := affected + row_count;

    RETURN affected;
END;
$$;

CREATE OR REPLACE FUNCTION analytics.refresh_dim_product() RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    affected BIGINT := 0;
    row_count BIGINT := 0;
BEGIN
    UPDATE analytics.dim_product d
    SET valid_to = timezone('UTC', now()),
        is_current = FALSE,
        updated_at = timezone('UTC', now())
    WHERE d.is_current = TRUE
      AND NOT EXISTS (
          SELECT 1
          FROM master.products s
          WHERE s.product_id::text = d.product_id
      );
    GET DIAGNOSTICS row_count = ROW_COUNT;
    affected := affected + row_count;

    UPDATE analytics.dim_product d
    SET valid_to = timezone('UTC', now()),
        is_current = FALSE,
        updated_at = timezone('UTC', now())
    FROM master.products s
    WHERE d.product_id = s.product_id::text
      AND d.is_current = TRUE
      AND (
          COALESCE(d.supplier_id, '') <> COALESCE(s.supplier_id::text, '')
          OR COALESCE(d.brand_name, '') <> COALESCE(s.brand_name, '')
          OR COALESCE(d.generic_name, '') <> COALESCE(s.generic_name, '')
          OR COALESCE(d.form_factor, '') <> COALESCE(s.form_factor::text, '')
          OR COALESCE(d.hsn_code, '') <> COALESCE(s.hsn_code, '')
      );
    GET DIAGNOSTICS row_count = ROW_COUNT;
    affected := affected + row_count;

    INSERT INTO analytics.dim_product (
        product_id,
        supplier_id,
        brand_name,
        generic_name,
        form_factor,
        hsn_code,
        valid_from,
        valid_to,
        is_current,
        created_at,
        updated_at
    )
    SELECT
        s.product_id::text,
        s.supplier_id::text,
        s.brand_name,
        s.generic_name,
        s.form_factor::text,
        s.hsn_code,
        timezone('UTC', now()),
        NULL,
        TRUE,
        timezone('UTC', now()),
        timezone('UTC', now())
    FROM master.products s
    LEFT JOIN analytics.dim_product d
      ON d.product_id = s.product_id::text
     AND d.is_current = TRUE
    WHERE d.product_id IS NULL
       OR COALESCE(d.supplier_id, '') <> COALESCE(s.supplier_id::text, '')
       OR COALESCE(d.brand_name, '') <> COALESCE(s.brand_name, '')
       OR COALESCE(d.generic_name, '') <> COALESCE(s.generic_name, '')
       OR COALESCE(d.form_factor, '') <> COALESCE(s.form_factor::text, '')
       OR COALESCE(d.hsn_code, '') <> COALESCE(s.hsn_code, '');
    GET DIAGNOSTICS row_count = ROW_COUNT;
    affected := affected + row_count;

    RETURN affected;
END;
$$;

CREATE OR REPLACE FUNCTION analytics.refresh_dim_warehouse() RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    affected BIGINT := 0;
    row_count BIGINT := 0;
BEGIN
    UPDATE analytics.dim_warehouse d
    SET valid_to = timezone('UTC', now()),
        is_current = FALSE,
        updated_at = timezone('UTC', now())
    WHERE d.is_current = TRUE
      AND NOT EXISTS (
          SELECT 1
          FROM master.warehouse_locations s
          WHERE s.location_id::text = d.warehouse_id
      );
    GET DIAGNOSTICS row_count = ROW_COUNT;
    affected := affected + row_count;

    UPDATE analytics.dim_warehouse d
    SET valid_to = timezone('UTC', now()),
        is_current = FALSE,
        updated_at = timezone('UTC', now())
    FROM master.warehouse_locations s
    WHERE d.warehouse_id = s.location_id::text
      AND d.is_current = TRUE
      AND (
          COALESCE(d.warehouse_name, '') <> COALESCE(s.name, '')
          OR COALESCE(d.temperature_range, '') <> COALESCE(s.temperature_range, '')
      );
    GET DIAGNOSTICS row_count = ROW_COUNT;
    affected := affected + row_count;

    INSERT INTO analytics.dim_warehouse (
        warehouse_id,
        warehouse_name,
        temperature_range,
        valid_from,
        valid_to,
        is_current,
        created_at,
        updated_at
    )
    SELECT
        s.location_id::text,
        s.name,
        s.temperature_range,
        timezone('UTC', now()),
        NULL,
        TRUE,
        timezone('UTC', now()),
        timezone('UTC', now())
    FROM master.warehouse_locations s
    LEFT JOIN analytics.dim_warehouse d
      ON d.warehouse_id = s.location_id::text
     AND d.is_current = TRUE
    WHERE d.warehouse_id IS NULL
       OR COALESCE(d.warehouse_name, '') <> COALESCE(s.name, '')
       OR COALESCE(d.temperature_range, '') <> COALESCE(s.temperature_range, '');
    GET DIAGNOSTICS row_count = ROW_COUNT;
    affected := affected + row_count;

    RETURN affected;
END;
$$;

CREATE OR REPLACE FUNCTION analytics.refresh_inventory_dimensions() RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    total BIGINT := 0;
BEGIN
    total := total + analytics.refresh_dim_supplier();
    total := total + analytics.refresh_dim_product();
    total := total + analytics.refresh_dim_warehouse();
    RETURN total;
END;
$$;
