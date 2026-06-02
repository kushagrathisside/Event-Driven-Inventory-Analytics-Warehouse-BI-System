-- ============================================================
-- ANALYTICAL DOMAINS — FDW Extensions + Read-Only Views
-- Covers: Prescription, Supplier Management, Financial AP/AR,
--         Customer Analytics, Compliance/Audit, Staff Performance
-- These domains read from master DB via FDW — no Kafka pipeline needed.
-- ============================================================

-- --------------------------------------------------------
-- EXTEND FDW: expose remaining master tables
-- --------------------------------------------------------
DROP FOREIGN TABLE IF EXISTS master.prescriptions CASCADE;
DROP FOREIGN TABLE IF EXISTS master.prescription_items CASCADE;
DROP FOREIGN TABLE IF EXISTS master.payments CASCADE;
DROP FOREIGN TABLE IF EXISTS master.customers CASCADE;
DROP FOREIGN TABLE IF EXISTS master.users CASCADE;
DROP FOREIGN TABLE IF EXISTS master.audit_logs CASCADE;

IMPORT FOREIGN SCHEMA public
LIMIT TO (
    prescriptions,
    prescription_items,
    payments,
    customers,
    users,
    audit_logs
)
FROM SERVER medwarehouse_master_server
INTO master;

REVOKE ALL ON FOREIGN TABLE master.prescriptions       FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.prescription_items  FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.payments            FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.customers           FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.users               FROM PUBLIC;
REVOKE ALL ON FOREIGN TABLE master.audit_logs          FROM PUBLIC;

GRANT SELECT ON master.prescriptions       TO {{FDW_READER_USER}};
GRANT SELECT ON master.prescription_items  TO {{FDW_READER_USER}};
GRANT SELECT ON master.payments            TO {{FDW_READER_USER}};
GRANT SELECT ON master.customers           TO {{FDW_READER_USER}};
-- users table: restricted — only expose safe columns via view (no password_hash)
GRANT SELECT ON master.audit_logs          TO {{FDW_READER_USER}};


-- --------------------------------------------------------
-- DOMAIN: PRESCRIPTION ANALYTICS & COMPLIANCE
-- --------------------------------------------------------

-- Prescription fill rate: how many prescriptions resulted in a completed sale
CREATE OR REPLACE VIEW analytics.v_prescription_fill_rate AS
SELECT
    p.prescription_id,
    p.customer_id,
    c.full_name AS customer_name,
    p.doctor_name,
    p.prescription_date,
    COUNT(pi.prescription_item_id)    AS prescribed_products,
    COUNT(DISTINCT s.sale_id)         AS linked_sales,
    CASE WHEN COUNT(DISTINCT s.sale_id) > 0 THEN TRUE ELSE FALSE END AS fulfilled
FROM master.prescriptions p
LEFT JOIN master.prescription_items pi  ON pi.prescription_id = p.prescription_id
LEFT JOIN master.sales s                ON s.prescription_id  = p.prescription_id
                                       AND s.status = 'completed'
LEFT JOIN master.customers c            ON c.customer_id = p.customer_id
GROUP BY p.prescription_id, p.customer_id, c.full_name, p.doctor_name, p.prescription_date;


-- Controlled substance dispensing register (Schedule H1, X, NDPS — regulatory requirement)
CREATE OR REPLACE VIEW analytics.v_controlled_substance_register AS
SELECT
    s.sale_id,
    s.invoice_number,
    s.sale_timestamp,
    si.product_id::text                 AS product_id,
    pr.brand_name,
    pr.generic_name,
    pr.drug_schedule::text              AS drug_schedule,
    si.quantity_sold,
    si.mrp_at_sale,
    si.price_per_unit_at_sale,
    s.customer_id::text                 AS customer_id,
    c.full_name                         AS customer_name,
    c.government_id,
    s.prescription_id::text             AS prescription_id,
    p.doctor_name,
    p.prescription_date,
    CASE WHEN s.prescription_id IS NULL THEN TRUE ELSE FALSE END AS missing_prescription,
    u.username                          AS dispensed_by
FROM master.sales s
JOIN master.sale_items si              ON si.sale_id    = s.sale_id
JOIN master.products pr                ON pr.product_id = si.product_id
LEFT JOIN master.customers c           ON c.customer_id  = s.customer_id
LEFT JOIN master.prescriptions p       ON p.prescription_id = s.prescription_id
LEFT JOIN master.users u               ON u.user_id = s.user_id
WHERE pr.drug_schedule IN ('Schedule H1', 'Schedule X', 'NDPS')
  AND s.status = 'completed'
ORDER BY s.sale_timestamp DESC;


-- Compliance flag: Schedule H1/X/NDPS dispensed without a prescription (regulatory violation)
CREATE OR REPLACE VIEW analytics.v_prescription_compliance_violations AS
SELECT *
FROM analytics.v_controlled_substance_register
WHERE missing_prescription = TRUE;


-- Doctor prescribing patterns
CREATE OR REPLACE VIEW analytics.v_doctor_prescribing_patterns AS
SELECT
    p.doctor_name,
    pi.product_id::text         AS product_id,
    pr.brand_name,
    pr.generic_name,
    pr.drug_schedule::text      AS drug_schedule,
    pr.therapeutic_class,
    COUNT(DISTINCT p.prescription_id)   AS total_prescriptions,
    MAX(p.prescription_date)            AS last_prescription_date
FROM master.prescriptions p
JOIN master.prescription_items pi  ON pi.prescription_id = p.prescription_id
JOIN master.products pr            ON pr.product_id = pi.product_id
GROUP BY p.doctor_name, pi.product_id, pr.brand_name, pr.generic_name,
         pr.drug_schedule, pr.therapeutic_class
ORDER BY total_prescriptions DESC;


-- --------------------------------------------------------
-- DOMAIN: SUPPLIER MANAGEMENT
-- --------------------------------------------------------

-- Supplier license expiry calendar (critical — cannot source from unlicensed supplier)
CREATE OR REPLACE VIEW analytics.v_supplier_license_expiry AS
SELECT
    s.supplier_id::text                     AS supplier_id,
    s.name                                  AS supplier_name,
    s.drug_license_no,
    s.license_expiry_date,
    s.gstin,
    s.supplier_type::text                   AS supplier_type,
    s.status::text                          AS supplier_status,
    CURRENT_DATE                            AS checked_date,
    s.license_expiry_date - CURRENT_DATE    AS days_until_expiry,
    CASE
        WHEN s.license_expiry_date < CURRENT_DATE          THEN 'EXPIRED'
        WHEN s.license_expiry_date <= CURRENT_DATE + 30    THEN 'CRITICAL'
        WHEN s.license_expiry_date <= CURRENT_DATE + 90    THEN 'WARNING'
        ELSE 'VALID'
    END                                     AS license_status,
    sc.contact_name                         AS primary_contact,
    sc.phone                                AS primary_phone,
    sc.email                                AS primary_email
FROM master.suppliers s
LEFT JOIN master.supplier_contacts sc
    ON sc.supplier_id = s.supplier_id AND sc.is_primary = TRUE
WHERE s.status = 'active'
ORDER BY days_until_expiry;


-- Active suppliers with full contact + address details
CREATE OR REPLACE VIEW analytics.v_supplier_directory AS
SELECT
    s.supplier_id::text     AS supplier_id,
    s.name,
    s.gstin,
    s.drug_license_no,
    s.license_expiry_date,
    s.supplier_type::text   AS supplier_type,
    s.payment_terms,
    s.typical_lead_time_days,
    s.status::text          AS status,
    sc.contact_name,
    sc.role,
    sc.phone,
    sc.email,
    sa.address_line_1,
    sa.city,
    sa.state,
    sa.pincode
FROM master.suppliers s
LEFT JOIN master.supplier_contacts sc
    ON sc.supplier_id = s.supplier_id AND sc.is_primary = TRUE
LEFT JOIN master.supplier_addresses sa
    ON sa.supplier_id = s.supplier_id AND sa.type = 'shipping'
WHERE s.status = 'active';


-- --------------------------------------------------------
-- DOMAIN: FINANCIAL ANALYTICS (AP/AR)
-- --------------------------------------------------------

-- Accounts payable: outstanding PO payments
CREATE OR REPLACE VIEW analytics.v_accounts_payable AS
SELECT
    po.po_id::text              AS po_id,
    po.po_number,
    po.supplier_id::text        AS supplier_id,
    s.name                      AS supplier_name,
    po.order_date,
    po.status::text             AS po_status,
    SUM(poi.line_total)         AS po_total_amount,
    COALESCE(SUM(pay.amount), 0) AS amount_paid,
    SUM(poi.line_total) - COALESCE(SUM(pay.amount), 0) AS outstanding_amount,
    CURRENT_DATE - po.order_date AS age_days
FROM master.purchase_orders po
JOIN master.purchase_order_items poi ON poi.po_id = po.po_id
LEFT JOIN master.payments pay
    ON pay.related_po_id = po.po_id AND pay.status = 'completed'
LEFT JOIN master.suppliers s ON s.supplier_id = po.supplier_id
WHERE po.status NOT IN ('cancelled')
GROUP BY po.po_id, po.po_number, po.supplier_id, s.name, po.order_date, po.status
HAVING SUM(poi.line_total) - COALESCE(SUM(pay.amount), 0) > 0
ORDER BY outstanding_amount DESC;


-- Payment method distribution (cash flow and payment mode analytics)
CREATE OR REPLACE VIEW analytics.v_payment_method_breakdown AS
SELECT
    date_trunc('month', pay.payment_date)::date     AS month,
    pay.payer_type,
    pay.payment_method::text                        AS payment_method,
    pay.status,
    COUNT(*)                                        AS transaction_count,
    SUM(pay.amount)                                 AS total_amount
FROM master.payments pay
GROUP BY date_trunc('month', pay.payment_date)::date,
         pay.payer_type, pay.payment_method, pay.status
ORDER BY month DESC, total_amount DESC;


-- Daily collections summary (customer-side payments)
CREATE OR REPLACE VIEW analytics.v_daily_collections AS
SELECT
    pay.payment_date::date          AS collection_date,
    pay.payment_method::text        AS payment_method,
    COUNT(*)                        AS payment_count,
    SUM(pay.amount)                 AS total_collected
FROM master.payments pay
WHERE pay.payer_type = 'customer'
  AND pay.status = 'completed'
GROUP BY pay.payment_date::date, pay.payment_method
ORDER BY collection_date DESC;


-- --------------------------------------------------------
-- DOMAIN: CUSTOMER ANALYTICS
-- --------------------------------------------------------

-- Customer lifetime value and purchase summary
CREATE OR REPLACE VIEW analytics.v_customer_summary AS
SELECT
    c.customer_id::text         AS customer_id,
    c.full_name,
    c.phone,
    c.email,
    c.date_of_birth,
    EXTRACT(YEAR FROM AGE(c.date_of_birth))::int AS age_years,
    c.government_id IS NOT NULL AS id_verified,
    COUNT(DISTINCT s.sale_id)   AS total_purchases,
    SUM(s.total_amount_payable) FILTER (WHERE s.status = 'completed') AS lifetime_value,
    MAX(s.sale_timestamp)       AS last_purchase_date,
    MIN(s.sale_timestamp)       AS first_purchase_date,
    COUNT(DISTINCT p.prescription_id) AS total_prescriptions
FROM master.customers c
LEFT JOIN master.sales s            ON s.customer_id = c.customer_id
LEFT JOIN master.prescriptions p    ON p.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name, c.phone, c.email,
         c.date_of_birth, c.government_id;


-- --------------------------------------------------------
-- DOMAIN: COMPLIANCE & AUDIT
-- --------------------------------------------------------

-- Audit activity log (no password_hash — safe for analysts)
CREATE OR REPLACE VIEW analytics.v_audit_activity AS
SELECT
    al.log_id,
    al.timestamp,
    al.user_id::text    AS user_id,
    u.username,
    u.role::text        AS user_role,
    al.action,
    al.entity,
    al.entity_id::text  AS entity_id,
    al.old_values,
    al.new_values,
    al.ip_address
FROM master.audit_logs al
LEFT JOIN master.users u ON u.user_id = al.user_id
ORDER BY al.timestamp DESC;


-- Recalled and banned product sales (critical compliance check)
CREATE OR REPLACE VIEW analytics.v_recalled_product_sales AS
SELECT
    si.sale_item_id::text   AS sale_item_id,
    s.sale_id::text         AS sale_id,
    s.invoice_number,
    s.sale_timestamp,
    si.product_id::text     AS product_id,
    pr.brand_name,
    pr.generic_name,
    pr.status::text         AS product_status,
    si.quantity_sold,
    si.line_total,
    c.full_name             AS customer_name,
    u.username              AS dispensed_by
FROM master.sales s
JOIN master.sale_items si          ON si.sale_id    = s.sale_id
JOIN master.products pr            ON pr.product_id = si.product_id
LEFT JOIN master.customers c       ON c.customer_id = s.customer_id
LEFT JOIN master.users u           ON u.user_id     = s.user_id
WHERE pr.status IN ('recalled', 'banned')
ORDER BY s.sale_timestamp DESC;


-- --------------------------------------------------------
-- DOMAIN: STAFF PERFORMANCE
-- --------------------------------------------------------

-- Staff sales performance (revenue and transaction counts per user)
CREATE OR REPLACE VIEW analytics.v_staff_performance AS
SELECT
    u.user_id::text         AS user_id,
    u.username,
    u.full_name,
    u.role::text            AS role,
    COUNT(DISTINCT s.sale_id) FILTER (WHERE s.status = 'completed')  AS completed_sales,
    COUNT(DISTINCT s.sale_id) FILTER (WHERE s.status = 'returned')   AS returns_processed,
    SUM(s.total_amount_payable) FILTER (WHERE s.status = 'completed') AS total_revenue,
    COUNT(DISTINCT sa.adjustment_id)                                  AS stock_adjustments_made,
    COUNT(DISTINCT gr.grn_id)                                         AS grns_received,
    u.last_login,
    u.status::text          AS account_status
FROM master.users u
LEFT JOIN master.sales s                ON s.user_id    = u.user_id
LEFT JOIN master.stock_adjustments sa   ON sa.adjusted_by = u.user_id
LEFT JOIN master.goods_receipts gr      ON gr.received_by  = u.user_id
WHERE u.status = 'active'
GROUP BY u.user_id, u.username, u.full_name, u.role, u.last_login, u.status;


-- Inactive accounts risk (active status but no login > 90 days — security risk)
CREATE OR REPLACE VIEW analytics.v_inactive_user_accounts AS
SELECT
    user_id::text   AS user_id,
    username,
    full_name,
    role::text      AS role,
    last_login,
    CURRENT_DATE - last_login::date AS days_since_login,
    status::text    AS status
FROM master.users
WHERE status = 'active'
  AND (last_login IS NULL OR last_login < now() - interval '90 days')
ORDER BY days_since_login DESC NULLS FIRST;


-- --------------------------------------------------------
-- GRANTS FOR ALL NEW VIEWS
-- --------------------------------------------------------
GRANT SELECT ON analytics.v_prescription_fill_rate           TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_controlled_substance_register    TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_prescription_compliance_violations TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_doctor_prescribing_patterns      TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_supplier_license_expiry          TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_supplier_directory               TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_accounts_payable                 TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_payment_method_breakdown         TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_daily_collections                TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_customer_summary                 TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_audit_activity                   TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_recalled_product_sales           TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_staff_performance                TO {{ANALYTICS_READER_USER}};
GRANT SELECT ON analytics.v_inactive_user_accounts           TO {{ANALYTICS_READER_USER}};
