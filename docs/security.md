# Security Model

---

## Authentication

### API Key Authentication
Both services use static API key authentication via HTTP headers.

**Header formats accepted:**
```
X-API-Key: <key>
Authorization: Bearer <key>
```

**Configuration:**
```bash
MW_WEBAPP_API_KEY=<hex-32-bytes>     # Operator webapp (port 8080)
MW_PLATFORM_API_KEY=<hex-32-bytes>   # Monitoring platform (port 8787)
```

**Generate a key:**
```bash
openssl rand -hex 32
```

**Constant-time comparison:** Keys are compared using `hmac.compare_digest()` rather than the `==` operator. This prevents timing-based side-channel attacks where an attacker could infer the key length or prefix by measuring response latency differences.

**Exemptions:** The `/health` endpoint and `/static/*` paths are always accessible without authentication.

**Behaviour when no key is configured:** Auth is skipped entirely and a `SECURITY WARNING` is logged at startup. This is acceptable for local development only.

---

## Database Role Model

Four PostgreSQL roles are used, following least-privilege principles:

| Role | Database | Privileges | Used By |
|---|---|---|---|
| `postgres` (superuser) | Both | Unrestricted | Bootstrap scripts only |
| `spark_writer` | analytics | SELECT/INSERT/TRUNCATE on staging, INSERT on facts, EXECUTE on load functions | Spark jobs, webapp API calls that trigger pipeline |
| `analytics_reader` | analytics | SELECT on views, EXECUTE on quality check functions | Power BI, BI tools, read-only API |
| `fdw_reader` | master | SELECT on all exposed tables | FDW server user mapping |

The `analytics_reader` role **cannot** write any data. It cannot access raw fact tables directly — only through views. It cannot access the master database tables directly.

The `fdw_reader` role on the master database has `SELECT` only. It has no INSERT, UPDATE, or DELETE privileges.

---

## Credential Management

**Default credentials (development only):**
All default credentials are documented in `.env.example`. They are intentionally weak (e.g., `postgres`/`postgres`) for local setup convenience.

**Production enforcement:**
The `get_settings()` function in `config.py` calls `_warn_weak_credentials()` at startup:
- In `local` / `dev` / `development` / `test` environments: logs a `WARNING` and continues
- In all other environments: raises a `RuntimeError` and prevents startup

**Credential rotation:**
Credentials are set in the `.env` file and passed to PostgreSQL at bootstrap via SQL template substitution. To rotate:
1. Update the relevant `MW_*_PASSWORD` variable in `.env`
2. Run `python -m medwarehouse warehouse bootstrap` to update the role's password in PostgreSQL

---

## Sensitive Data Handling

**Password hashes:** The `users.password_hash` column in the master database is **never** exposed through analytics views. The `v_audit_activity` view uses `master.users` but explicitly excludes `password_hash`.

**Bank account numbers:** `supplier_bank_details` is not exposed in any analytics FDW view. It remains only in the OLTP system.

**Government IDs / PII:** The `customers.government_id` field is available in `v_customer_summary` because it is needed for controlled substance purchase verification. Grant the `analytics_reader` role only to users who are authorised to see customer PII.

**Database dump:** The `meddata_full_20250916_175921.dump` file in the repository root is an AI-generated sample dataset. It is listed in `.gitignore`. Do not commit real operational data dumps to version control.

---

## Regulatory Compliance

This system handles pharmaceutical inventory, which is subject to Indian drug regulations:

**Schedule H, H1, X, and NDPS drugs:**
- Sales must be linked to a valid prescription (`sales.prescription_id IS NOT NULL`)
- The `v_prescription_compliance_violations` view surfaces any sales of these drugs without a prescription
- The `ControlledSubstanceComplianceRule` alert fires if any violations are detected
- The `v_controlled_substance_register` view provides the dispensing register required by law

**Supplier drug licenses:**
- No procurement from suppliers with an expired `drug_license_no`
- The `v_supplier_license_expiry` view flags EXPIRED, CRITICAL (≤30 days), and WARNING (≤90 days) licenses
- The `SupplierLicenseExpiryRule` alert fires automatically when licenses approach expiry
- The alert fires as CRITICAL when a license is expired or within 30 days

**Audit trails:**
- All entity changes are recorded in `audit_logs` in the master database
- The `v_audit_activity` analytics view provides analyst-safe access (no password data)
- Audit logs are immutable — the analytics system does not modify them

---

## Network Security

**Kafka:** The local Kafka broker uses plaintext listeners with no authentication. In a production deployment, configure SASL/SCRAM or mTLS.

**PostgreSQL:** In the Docker setup, PostgreSQL is bound to all interfaces (`0.0.0.0:5432`). For production, restrict to specific IP ranges using `pg_hba.conf`.

**Flask services:** Both the monitoring platform and operator webapp default to `127.0.0.1`. Use `--host 0.0.0.0` only behind a reverse proxy (nginx, Traefik) that handles TLS termination.

---

## Known Security Gaps (Tracked)

| Gap | Severity | Status | Mitigation |
|---|---|---|---|
| No TLS on Kafka | Medium | Open | Acceptable for internal/single-node deployments; configure SASL/SCRAM or mTLS for multi-node |
| No session-based auth (stateless API key only) | Low | Open | Add JWT or session tokens if per-user auditing is required |
| Airflow default credentials | High | **Configurable** | Set `AIRFLOW_ADMIN_PASSWORD` in `.env` before first `docker compose up`; also set `AIRFLOW__WEBSERVER__SECRET_KEY` for production |
| No rate limiting on Flask APIs | Low | Open | Add `Flask-Limiter` for production deployments |
| Flask pinned to 2.x in Airflow container | Low | Open | Upgrade Airflow image to 3.x when dependency constraints allow |
| Idempotency store is in-memory | Low | Open | Current `IdempotencyStore` uses a per-process dict with background TTL eviction; replace with Redis for multi-process or multi-instance deployments |
| PostgreSQL binds to `0.0.0.0` in Docker | Medium | Open | Restrict to specific IP ranges via `pg_hba.conf` or Docker network isolation for non-development deployments |
