# Data Lineage Specification (OpenLineage) — VERSION_2

**Version:** 2.0 (Production)  
**Scope:** Lakehouse Dota 2 (RAW → STAGE → MART → Trino → Superset)  
**Regulatory Driver:** None (educational project). The architecture is designed with best practices in mind (analogous to GDPR, BCBS 239) solely to foster proper engineering habits. Full compliance with regulatory requirements is not implemented.  
**Tool:** Apache OpenLineage → Marquez  
**Relevance:** This document describes lineage **only for the implemented Case #3 pipeline**.

---

## 1. Purpose

Data lineage ensures full traceability:
- which tables the data was sourced from;
- which filters were applied;
- which transformations were executed;
- which business metrics were calculated;
- in which final mart the result was stored.

Tracing is required for auditing, debugging, incident investigation, and regulatory compliance.

---

## 2. Lineage Architecture

```text
Source (PG/Mongo) → Airflow DAG → Spark Job → Iceberg RAW → Iceberg STAGE → Iceberg MART → Trino → Superset
      │                                                                          │
      ▼                                                                          ▼
Marquez (OpenLineage) ←──────────────────────────────────────────────────────────┘
```

- **PySpark Jobs:** emit lineage events via the custom `ctx.lineage()` context manager from `src/common/lakehouse_spark.py`.
- **Airflow DAGs:** automatically emit lineage events on task start/completion.
- **Marquez:** stores and visualizes the dependency graph of datasets and jobs.

---

## 3. Lineage Event Format

Each OpenLineage event contains:

| Field | Description | Example |
|-------|-------------|---------|
| `job.name` | Job name | `practice-case-03-stage-clean-matches` |
| `job.namespace` | Namespace | `bootcamp2026.case03` |
| `run.runId` | Unique run ID | UUID |
| `inputs` | List of input datasets | `["lakehouse.raw.pg__matches"]` |
| `outputs` | List of output datasets | `["lakehouse.stage.case03_pg__matches"]` |
| `description` | Transformation description | `Deduplication and cleaning of matches` |
| `producer` | Tool that created the event | `bootcamp2026-notebooks/VERSION_2` |
| `timestamp` | Event time | ISO 8601 |

---

## 4. PySpark Implementation

Lineage is implemented via the `ctx.lineage()` context manager:

```python
with ctx.lineage(
    job_name="practice-case-03-stage-clean-matches",
    inputs=["lakehouse.raw.pg__matches"],
    output="lakehouse.stage.case03_pg__matches",
    description="Deduplication and cleaning of matches (no business aggregations)",
    dataset_descriptions={
        "lakehouse.stage.case03_pg__matches": "Clean matches table >= 10 min and 10 players."
    }
):
    clean_matches.writeTo("lakehouse.stage.case03_pg__matches").using("iceberg").createOrReplace()
```

**Rules:**
1. Every read from a source and write to a layer must be wrapped in `ctx.lineage()`.
2. `inputs` — all tables from which data is read.
3. `outputs` — all tables to which data is written.
4. `dataset_descriptions` — brief description of each output dataset.

---

## 5. Dataset Map for Case #3

| Dataset | Layer | Sources | Transformations | Consumers |
|---------|-------|---------|-----------------|-----------|
| `lakehouse.raw.pg__matches` | RAW | PostgreSQL `dota.matches` | Idempotent MERGE INTO | STAGE |
| `lakehouse.raw.pg__match_players` | RAW | PostgreSQL `dota.match_players` | Idempotent MERGE INTO | STAGE |
| `lakehouse.raw.mongo__cs_events` | RAW | MongoDB `dota_events.cs_events` | Filter `npcId=133`, MERGE INTO | STAGE |
| `lakehouse.raw.mongo__gold_events` | RAW | MongoDB `dota_events.gold_events` | Filter `reason='ROSHAN'`, MERGE INTO | STAGE |
| `lakehouse.raw.mongo__tower_death_events` | RAW | MongoDB `dota_events.tower_death_events` | Idempotent MERGE INTO | STAGE |
| `lakehouse.stage.case03_pg__matches` | STAGE | `raw.pg__matches` | Deduplication, validation, filtering | MART |
| `lakehouse.stage.case03_pg__match_players` | STAGE | `raw.pg__match_players` + `stage.case03_pg__matches` | Deduplication, referential integrity | MART |
| `lakehouse.stage.case03_mongo__cs_events` | STAGE | `raw.mongo__cs_events` + `stage.case03_pg__matches` | Time validation, deduplication | MART |
| `lakehouse.stage.case03_mongo__gold_events` | STAGE | `raw.mongo__gold_events` + `stage.case03_pg__matches` | Time validation, deduplication | MART |
| `lakehouse.stage.case03_mongo__tower_death_events` | STAGE | `raw.mongo__tower_death_events` + `stage.case03_pg__matches` | Time validation, deduplication | MART |
| `lakehouse.mart.case03_pg__match_roshan_mart` | MART | 5 STAGE tables | Cascading side resolution, towers, aggregates | Trino, Superset, DQ |
| `lakehouse.mart.case03_pg__roshan_time_effect` | MART | `mart.case03_pg__match_roshan_mart` | Aggregation by intervals, quantiles | Trino, Superset, DQ |
| `lakehouse.mart.case03_pg__roshan_control_effect` | MART | `mart.case03_pg__match_roshan_mart` | Aggregation by control and match pace | Trino, Superset, DQ |

---

## 6. Lineage Integrity Checks

Mandatory checks before mart publication:
- [ ] **All inputs reflected:** Every input dataset in code is listed in `ctx.lineage(inputs=...)`.
- [ ] **All outputs reflected:** Every written table is listed in `ctx.lineage(output=...)`.
- [ ] **No broken links:** Every MART dataset has an unbroken lineage chain back to RAW.
- [ ] **Descriptions filled:** `dataset_descriptions` are populated for all outputs.

---

## 7. Connection to DQ and Incidents

When an issue is detected in a mart:
1. Locate the mart in the Marquez UI.
2. Build the upstream graph back to the source.
3. Determine which transformation or source caused the anomaly.
4. Log the finding in the incident report (see `04_incident_response_runbook-en.md`).

---

## 8. Ownership & Maintenance

**Owner:** Data Engineering team.  
**Update Trigger:** Addition of new tables, schema changes, or new use cases.  
**Review Frequency:** Monthly.