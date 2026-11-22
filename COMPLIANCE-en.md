# Compliance and Data Governance

This document outlines the measures implemented to ensure data quality, security, reproducibility, and observability in **VERSION_2** of the project.

> **Important:** This document reflects **only the mechanisms actually implemented** in the codebase. Any features not deployed in the project (e.g., cryptographic encryption of data at rest or automated data retention) are intentionally omitted to avoid misleading stakeholders.

---

## 1. Data Quality (DQ)

The pipeline enforces strict quality checks at every layer of the Medallion Architecture (RAW → STAGE → MART) using the `lakehouse_dq.py` engine.

### Severity Levels:
- **HARD:** The pipeline halts immediately upon failure; data is not published to the MART layer. Examples:
  - Missing mandatory fields (`match_id`, `radiant_win`).
  - Primary key uniqueness violations after deduplication.
  - Invalid data types or out-of-bounds values (e.g., `duration < 600` seconds).
- **QUARANTINE:** Malformed records are isolated into dedicated tables (`lakehouse.quarantine.*`), while clean data proceeds through the pipeline. Examples:
  - Matches with incomplete rosters (`human_players != 10`).
  - Events with invalid timestamps (`time < 0` or `time > duration`).

### Key Checks for Case #3 (Roshan):
1. **Sum Integrity:** `roshan_killed_by_radiant + roshan_killed_by_dire + roshan_killer_side_unknown == total_roshan_kills`.
2. **Timing Validity:** The interval between consecutive Roshan kills in a single match must be ≥ 480 seconds.
3. **Referential Integrity:** 100% of records in STAGE must have valid `match_id` and `hero_id` that exist in the reference dictionaries.

---

## 2. Personal Data Protection (PII & GDPR/152-FZ Principles)

The project processes data that may be classified as pseudonymized personal data (Steam `account_id`, `personaname`).

### Implemented Measures:
- **Data Minimization:** Direct identifiers (`personaname`) are not propagated to the analytical MART layer. Only `account_id` is used as a surrogate key for aggregation.
- **Anonymous Filtering:** Records with `account_id = 4294967295` (anonymous Steam profiles) are explicitly filtered out during the STAGE cleaning process.
- **No Encryption:** Within the scope of this educational project, data in PostgreSQL, MongoDB, and MinIO is stored in plaintext. Cryptographic encryption at rest or in transit is **not implemented**.

---

## 3. Observability and Data Lineage

To ensure transformation traceability, the project utilizes the **OpenLineage** standard, visualized via **Marquez**.

### Implemented Mechanisms:
- Every PySpark job step (reading from RAW, writing to STAGE/MART) is wrapped in a `ctx.lineage()` context manager.
- Lineage events capture:
  - Job Name.
  - Input Datasets.
  - Output Datasets.
  - A description of the transformation performed.
- This enables rapid root-cause analysis during incidents, allowing engineers to trace corrupted data in a MART table back to its specific source or transformation step.

---

## 4. Model Risk Management (MRM)

Currently, **VERSION_2** implements only a SQL/PySpark analytical pipeline (Case #3). Machine Learning (ALS, LLM) models are not deployed in this version.

### Applicable Measures for the Current Case:
- **Statistical Validation:** All hypotheses are tested with calculated p-values and 95% Wilson confidence intervals.
- **Leakage Prevention:** Aggregation features are calculated strictly based on historical STAGE data, without using post-factum metrics unavailable at the time of the event.
- **Reproducibility:** All transformations are deterministic. The data generator uses a fixed seed (`--seed`), guaranteeing identical datasets across repeated runs.

---

## 5. Access Control

Access management is implemented at a baseline level appropriate for a local educational environment.

### Implemented Mechanisms:
- **Network Isolation:** Core services (PostgreSQL, Mongo, MinIO, Airflow) are bound strictly to `127.0.0.1` (localhost), preventing unauthorized external network access.
- **Authentication:** Access to Airflow and Superset is protected by basic authentication (username/password) configured via `.env` variables.
- **No Fine-Grained RBAC:** Detailed Role-Based Access Control (e.g., Row-Level Security in Trino/Superset) is **not configured**. All authenticated users have equal access to the MART tables.

---

## 6. Incident Response

In the event of a pipeline failure (e.g., an Airflow task failing due to a HARD DQ rule violation):

1. **Automatic Halt:** The pipeline aborts; corrupted data is not written to the target Iceberg tables.
2. **Logging:** The root cause (e.g., `[DQ HARD FAIL] ... violated in 15/10000 rows`) is recorded in the Airflow task logs.
3. **Isolation:** If the failure is triggered by a QUARANTINE rule, the malformed records are safely stored in `lakehouse.quarantine.*` for manual review.
4. **Recovery:** After fixing the data source or transformation code, the DAG can be restarted from the failed task. Thanks to the idempotency of the operations (`MERGE INTO` in RAW and `createOrReplace` in STAGE/MART), rerunning will not create duplicate records.

---

## 7. Architectural Limitations (Honest Disclosure)

For full transparency, the following mechanisms are **absent** from the current implementation:

| Mechanism | Status | Justification / Compensating Control |
|---|---|---|
| **Incremental Loading in STAGE/MART** | ❌ Not Implemented | Full recomputation (`createOrReplace`) is used. This guarantees the correctness of non-additive aggregates (win rates, percentiles) with minimal maintenance overhead. Incremental loading is implemented only in the RAW layer. |
| **Automated Data Retention (Cleanup)** | ❌ Not Implemented | Audit tables (`cleanup_runs`) are created in the database schema, but automated cleanup jobs are not scheduled. Data accumulates indefinitely. |
| **Data Encryption (At Rest / In Transit)** | ❌ Not Implemented | This is an educational project deployed locally via Docker. |
| **Machine Learning (ML/LLM)** | ❌ Not Implemented | **VERSION_2** currently implements only Case #3 (SQL Analytics). Cases #20-23 remain as exploratory notebooks in `VERSION_1`. |
