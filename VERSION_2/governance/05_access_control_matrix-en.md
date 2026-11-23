# Access Control Matrix (RBAC) — VERSION_2

**Version:** 2.0 (Production)  
**Scope:** Lakehouse (RAW/STAGE/MART), Airflow, Trino, Superset  
**Principle:** Least privilege  
**Relevance:** Document adapted to the implemented ETL pipeline. Data Scientist and ML Engineer roles are excluded as ML/LLM cases are not implemented in VERSION_2.

---

## 1. Roles

| Role | Description | Composition |
|------|-------------|-------------|
| **Data Analyst** | Analytics, mart creation, research | Bootcamp participants, analysts |
| **Data Engineer** | ETL development, pipeline support | Data Engineering team |
| **BI Consumer** | Superset dashboard viewing | Business users, stakeholders |
| **System Admin** | Infrastructure and user administration | Platform administrators |
| **Audit / Compliance** | Auditing, compliance verification (role reserved for future checks) | Security, risk management |

---

## 2. Data Access Matrix (Lakehouse)

| Resource | Data Analyst | Data Engineer | BI Consumer | System Admin | Audit |
|----------|--------------|---------------|-------------|--------------|-------|
| **RAW** (Iceberg) | READ | READ/WRITE | — | — | READ |
| **STAGE** (Iceberg) | READ | READ/WRITE | — | — | READ |
| **MART** (Iceberg) | READ | READ/WRITE | READ | — | READ |
| **Quarantine tables** | — | READ/WRITE | — | — | READ |
| **Airflow DAGs** | — | READ/WRITE | — | — | READ |
| **Airflow Variables/Connections** | — | WRITE (secrets) | — | WRITE | READ |
| **Trino** | READ (MART) | READ/WRITE | READ (MART) | — | READ |
| **Superset** | READ | READ | READ/WRITE (dashboards) | — | READ |
| **Marquez (lineage)** | READ | READ/WRITE | READ | — | READ |
| **Iceberg snapshots** | READ | READ/WRITE | READ | — | READ |

**Legend:** READ — read access, WRITE — write/modify access, — — no access.

---

## 3. Operation Access Matrix

| Operation | Data Analyst | Data Engineer | BI Consumer | System Admin | Audit |
|-----------|--------------|---------------|-------------|--------------|-------|
| Manual DAG trigger | — | Yes | — | Yes | — |
| Iceberg snapshot rollback | — | Yes | — | — | — |
| MART creation | Yes | Yes | — | — | — |
| CSV data export | Yes (MART) | Yes | Yes (MART) | — | Yes |
| Table schema modification | — | Yes | — | — | — |
| User management | — | — | — | Yes | — |
| Incident log viewing | Yes | Yes | — | Yes | Yes |

---

## 4. PII Fields — Restrictions

| Field | Table | Who has access | Restrictions |
|-------|-------|----------------|--------------|
| `personaname` | `players` | Data Engineer, Audit | Do not export in plain text outside the platform. |
| `account_id` | `players`, `match_players` | All roles with data access | Pseudonymized, do not link to real persons without consent. |
| `steam_account_id` | Mongo collections | Data Engineer, Audit | Only within Iceberg/Spark. |

---

## 5. Access Provisioning Procedure

1. Access request via task tracker or directly to System Admin.
2. Approval by the resource owner (Data Engineering Lead for data, System Admin for users).
3. Provisioning of minimally necessary access.
4. Monthly access review.
5. Access revocation upon termination or role change (within 1 business day).

---

## 6. Access Monitoring

- Logging of all access attempts to RAW and PII fields.
- Weekly review of anomalous requests (mass download, off-hours access).
- Alerts on unauthorized access attempts.

---

## 7. Exceptions

Any exception to the matrix requires:
- Documented justification.
- Approval from System Admin and Data Engineering Lead.
- Limited validity period (maximum 30 days).
- Logging in the exceptions register.