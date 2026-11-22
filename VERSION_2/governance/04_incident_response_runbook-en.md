# Incident Response Runbook — Dota 2 Analytics (VERSION_2)

**Version:** 2.0 (Production)  
**Scope:** Lakehouse, Airflow, Spark, Trino, Superset  
**Base:** SLA (P1–P4 incident classification)  
**Relevance:** Adapted for the implemented ETL pipeline (Case #3). ML/LLM scenarios are excluded as not implemented.

---

## 1. Purpose

A unified procedure for incident response: logging, classification, escalation, resolution, recovery, and documentation.

---

## 2. Incident Classification

| Priority | Level | Description | Example | Response Time | Resolution Time |
|----------|-------|-------------|---------|---------------|-----------------|
| **P1** | Critical | Complete unavailability or substantial data inaccuracy | Mart corruption, massive calculation error | ≤ 30 min | ≤ 4 h |
| **P2** | High | Core pipeline functionality impaired | ETL failed, DQ HARD FAIL | ≤ 2 h | ≤ 8 h |
| **P3** | Medium | Partial error not preventing result usage | Incorrect calculation of a single metric | ≤ 8 business h | ≤ 2 business days |
| **P4** | Low | Minor documentation or presentation issue | Typo in Data Dictionary | ≤ 2 business days | ≤ 5 business days |

---

## 3. Response Procedure

### Step 1 — Issue Logging
Create a record in the incident log (Jira/GitHub Issues):
- `incident_id`: Unique ID
- `title`: Brief description
- `priority`: P1–P4
- `detected_at`: Detection time
- `affected_datasets`: Impacted tables/marts
- `description`: Problem description

### Step 2 — Priority Classification
Use the table in Section 2. If unclear, assign a higher priority.

### Step 3 — Escalation
- **P1:** Team Lead + Data Engineering Lead (Phone/messenger urgently)
- **P2:** Team Lead + Data Engineering Lead (Messenger + email)
- **P3:** Team Lead (Messenger)
- **P4:** Responsible Developer (Email / tracker task)

### Step 4 — Diagnostics
1. Check Airflow logs (DAG runs, task logs).
2. Check Spark logs (executor, driver).
3. Check DQ checks (which rules failed: HARD or QUARANTINE).
4. Check lineage in Marquez (where the "bad" data came from).
5. Check Iceberg snapshots (which mart version is corrupted).

### Step 5 — Containment
- Pause dependent pipelines.
- Do not delete the damaged mart — keep it for forensics.
- Roll back to a previous Iceberg snapshot if necessary.

### Step 6 — Resolution
- Fix code/data/configuration.
- Obtain approval from Data Engineering Lead.

### Step 7 — Quality Verification
After resolution:
1. Run all mandatory DQ checks for affected marts.
2. Ensure `is_sum_valid = true` (for Case #3).

### Step 8 — Recovery
- Re-run the pipeline.
- Verify the mart is accessible via Trino.
- Log the recovery time.

### Step 9 — Documentation
Fill out the incident report (root_cause, fix_description, lessons_learned).

---

## 4. Specific Scenarios

### Scenario A: DQ HARD FAIL in ETL
**Symptoms:** Airflow task failed with `[DQ HARD FAIL]`.  
**Actions:**
1. Identify the failed rule (rule_id, description).
2. Assess the number of violating rows.
3. Fix the source or cleaning rule.
4. Re-run from the same stage (idempotently via MERGE INTO or createOrReplace).

### Scenario B: DQ QUARANTINE
**Symptoms:** Rows isolated in `lakehouse.quarantine.*`.  
**Actions:**
1. Analyze why rows were quarantined.
2. If data can be recovered — fix and reload.
3. If data is lost — document as a loss.

### Scenario C: Iceberg Table Corruption
**Symptoms:** Read error, incorrect data in the mart.  
**Actions:**
1. Check Iceberg snapshots.
2. Roll back to the previous correct snapshot.
3. Check lineage (which transformation caused the corruption).
4. Re-run the transformation.

---

## 5. Connection to SLA

- Response and resolution times comply with SLA.
- Number of automatic retry attempts: up to 3.
- Interval between attempts: 5–15 min.
- Publication of an incorrect mart is prohibited.
- Post-run verification is mandatory.

---

## 6. Incident Log Template

```markdown
## Incident #{{id}}

| Field | Value |
|-------|-------|
| Detection Date | {{date}} |
| Priority | P{{1-4}} |
| Affected Marts | {{datasets}} |
| Root Cause | {{root_cause}} |
| Response Time | {{reaction_time}} |
| Resolution Time | {{resolution_time}} |
| Fix | {{fix}} |
| Prevention | {{prevention}} |
| Status | Closed / Open |
| Owner | {{owner}} |
```

---

## 7. Responsibilities

| Role | Duties |
|------|--------|
| Incident Manager | Response coordination, communications |
| Data Engineering Lead | Technical resolution of ETL incidents |
| Product Owner | Stakeholder communication, impact assessment |

---

## 8. Prevention

- Daily monitoring of DQ checks.
- Weekly review of failed Airflow jobs.
- Monthly incident review and lessons learned.
- Regular rollback testing on staging.