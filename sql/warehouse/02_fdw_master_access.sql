DROP FOREIGN TABLE IF EXISTS master.products CASCADE;
DROP FOREIGN TABLE IF EXISTS master.suppliers CASCADE;
DROP FOREIGN TABLE IF EXISTS master.warehouse_locations CASCADE;
DROP FOREIGN TABLE IF EXISTS master.goods_receipts CASCADE;
DROP FOREIGN TABLE IF EXISTS master.goods_receipt_items CASCADE;
DROP FOREIGN TABLE IF EXISTS master.stock_adjustments CASCADE;
DROP FOREIGN TABLE IF EXISTS master.sales CASCADE;
DROP FOREIGN TABLE IF EXISTS master.sale_items CASCADE;
DROP FOREIGN TABLE IF EXISTS master.purchase_orders CASCADE;
DROP FOREIGN TABLE IF EXISTS master.purchase_order_items CASCADE;

DROP SERVER IF EXISTS medwarehouse_master_server CASCADE;

CREATE SERVER medwarehouse_master_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host '{{MASTER_DB_HOST}}',
    dbname '{{MASTER_DB_NAME}}',
    port '{{MASTER_DB_PORT}}'
);

CREATE USER MAPPING FOR CURRENT_USER
SERVER medwarehouse_master_server
OPTIONS (
    user '{{FDW_REMOTE_USER}}',
    password '{{FDW_REMOTE_PASSWORD}}'
);

CREATE USER MAPPING FOR {{FDW_READER_USER}}
SERVER medwarehouse_master_server
OPTIONS (
    user '{{FDW_REMOTE_USER}}',
    password '{{FDW_REMOTE_PASSWORD}}'
);

-- analytics_reader and spark_writer need mappings so that plain (non-SECURITY DEFINER)
-- views in the analytics schema can query FDW foreign tables at query time.
CREATE USER MAPPING FOR {{ANALYTICS_READER_USER}}
SERVER medwarehouse_master_server
OPTIONS (
    user '{{FDW_REMOTE_USER}}',
    password '{{FDW_REMOTE_PASSWORD}}'
);

CREATE USER MAPPING FOR {{SPARK_WRITER_USER}}
SERVER medwarehouse_master_server
OPTIONS (
    user '{{FDW_REMOTE_USER}}',
    password '{{FDW_REMOTE_PASSWORD}}'
);

IMPORT FOREIGN SCHEMA public
LIMIT TO (
    products,
    suppliers,
    warehouse_locations,
    goods_receipts,
    goods_receipt_items,
    stock_adjustments,
    sales,
    sale_items,
    purchase_orders,
    purchase_order_items
)
FROM SERVER medwarehouse_master_server
INTO master;

REVOKE ALL ON FOREIGN TABLE master.products               FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.suppliers              FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.warehouse_locations    FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.goods_receipts         FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.goods_receipt_items    FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.stock_adjustments      FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.sales                  FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.sale_items             FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.purchase_orders        FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.purchase_order_items   FROM PUBLIC;

-- Grant FDW reader access to transaction tables (read-only from analytics side)
GRANT SELECT ON master.goods_receipts        TO {{FDW_READER_USER}};
GRANT SELECT ON master.goods_receipt_items   TO {{FDW_READER_USER}};
GRANT SELECT ON master.stock_adjustments     TO {{FDW_READER_USER}};
GRANT SELECT ON master.sales                 TO {{FDW_READER_USER}};
GRANT SELECT ON master.sale_items            TO {{FDW_READER_USER}};
GRANT SELECT ON master.purchase_orders       TO {{FDW_READER_USER}};
GRANT SELECT ON master.purchase_order_items  TO {{FDW_READER_USER}};
