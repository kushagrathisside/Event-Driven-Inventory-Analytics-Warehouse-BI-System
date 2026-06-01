DROP FOREIGN TABLE IF EXISTS master.products CASCADE;
DROP FOREIGN TABLE IF EXISTS master.suppliers CASCADE;
DROP FOREIGN TABLE IF EXISTS master.warehouse_locations CASCADE;

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

IMPORT FOREIGN SCHEMA public
LIMIT TO (products, suppliers, warehouse_locations)
FROM SERVER medwarehouse_master_server
INTO master;

REVOKE ALL ON FOREIGN TABLE master.products FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.suppliers FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.warehouse_locations FROM PUBLIC;
