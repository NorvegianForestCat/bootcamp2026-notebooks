# Data Quality (DQ) Specification for Lakehouse Dota 2 (VERSION_2)

This document describes the data quality control rules for the Medallion architecture (`RAW` → `STAGE` → `MART`) within the implemented pipeline for **Case #3: Roshan Kill Impact on Match Victory**.

---

## 1. Base Requirements by Lakehouse Layer

### Layer 1: Sources → `RAW` (Delivery Control)
Since PostgreSQL and MongoDB sources retain data for a limited time, it is critical to verify the completeness and timeliness of the data ingestion.

#### Timeliness (`DQ-RAW-01`)
* **Rule:** The maximum `loaded_at` value in `raw.pg__matches` must not lag behind the DAG run time by more than a defined threshold (safety buffer).
* **Reaction:** *Hard Fail* (pipeline halt and alert).

#### MongoDB Events Uniqueness (`DQ-RAW-02`)
* **Rule:** The string representation of `_id` (ObjectId) is unique across all event collections (`cs_events`, `gold_events`, `tower_death_events`).
* **Reaction:** *Hard Fail*.

---

### Layer 2: `RAW` → `STAGE` (Cleaning, Filtering, and Entity Validation)

#### Match and Player Slot Uniqueness (`DQ-STG-01`)
* **Rule:** `match_id` is unique in `stage.case03_pg__matches`; the composite key `(match_id, player_slot)` is unique in `stage.case03_pg__match_players`.
* **Reaction:** *Hard Fail* (after deduplication by `loaded_at DESC`).

#### Mandatory Match Fields (`DQ-STG-02`)
* **Rule:** Fields `match_id`, `start_time`, `duration`, `radiant_win` are strictly `NOT NULL`.
* **Reaction:** *Hard Fail*.

#### Match Duration and Composition Validation (`DQ-STG-03`)
* **Rule:** Abandoned/short matches are filtered out: `duration >= 600 AND duration <= 18000` (10 mins to 5 hours). Player count: `human_players = 10`.
* **Reaction:** *Quarantine* (filtered out of marts, logged in Data Funnel).

#### Slot and Hero Validation (`DQ-STG-04`)
* **Rule:** `player_slot IN (0, 1, 2, 3, 4, 128, 129, 130, 131, 132)`; `hero_id > 0`.
* **Reaction:** *Quarantine*.

#### 5v5 Composition Completeness (`DQ-STG-05`)
* **Rule:** For every valid `match_id` in `stage.case03_pg__match_players`, there are exactly 10 records (5 Radiant players and 5 Dire players).
* **Reaction:** *Quarantine*.

#### Event Game Time Validation (`DQ-STG-06`)
* **Rule:** The `time` field must be `>= 0` and `<= duration` of the match.
* **Reaction:** *Quarantine*.

---

### Layer 3: `STAGE` → `MART` (Referential Integrity and Business Logic)

#### Foreign Key Integrity (`DQ-MRT-01`)
* **Rule:** 100% of `hero_id` from `stage.case03_pg__match_players` exist in `stage.case03_pg__heroes`; 100% of `match_id` from event tables exist in `stage.case03_pg__matches`.
* **Reaction:** *Hard Fail* before mart assembly.

#### Case #3 Specific Checks (Roshan)

##### Roshan Kill Event Consistency (`DQ-CASE3-01`)
* **Rule:** For every Roshan last-hit event in `cs_events` (`npcId = 133`, `time > 0`), there must be exactly 5 team gold award records in `gold_events` (`reason = 'ROSHAN'`) for the winning side within a `±5 seconds` window in the same match.
* **Reaction:** *Quarantine* (exclude the match with desynchronized logs from calculations).

##### Roshan Respawn Timing Adequacy (`DQ-CASE3-02`)
* **Rule:** The first Roshan cannot be killed before creeps spawn (`time >= 0`). The interval between consecutive Roshan kills within a single `match_id` must be at least 480 seconds (minimum Roshan respawn timer is 8 minutes).
* **Reaction:** *Quarantine*.

##### Roshan Kill Sum Integrity in MART (`DQ-MRT-02`)
* **Rule:** For every record in `mart.case03_pg__match_roshan_mart`, the following condition must hold:  
  `roshan_killed_by_radiant + roshan_killed_by_dire + roshan_killer_side_unknown = total_roshan_kills`
* **Reaction:** *Hard Fail*.

##### Probability Range in MART (`DQ-MRT-03`)
* **Rule:** In the `mart.case03_pg__roshan_time_effect` mart, the `first_roshan_win_rate` field must be strictly within the range `[0, 1]`.
* **Reaction:** *Hard Fail*.

---

## 2. PySpark Implementation

Checks are implemented in the `src/jobs/case_3/dq_checks.py` module and integrated into the Airflow DAG as a separate `quality_layer`.

### DQ Engine Architecture
1. **HARD Rules:** If violations > 0, the pipeline halts with a `RuntimeError`, preventing the publication of incorrect data.
2. **QUARANTINE Rules:** Violating rows are isolated into `lakehouse.quarantine.*` tables with failure reasons (`dq_failed_rules`), while clean data continues through the pipeline.
3. **Data Funnel:** At the STAGE layer, the exact count and percentage of rows removed at each filtering step are logged.

### Check Code Example (DQ-MRT-02)
```python
def validate_mart_roshan_sum(spark: SparkSession) -> int:
    query = """
        SELECT count_if(
            (roshan_killed_by_radiant + roshan_killed_by_dire + roshan_killer_side_unknown) != total_roshan_kills
        ) as violations 
        FROM lakehouse.mart.case03_pg__match_roshan_mart
    """
    return spark.sql(query).first()["violations"]
```

---

## 3. Ownership & Maintenance

**Document Owner:** Data Engineering team.  
**Update Trigger:** Changes to mart business logic or addition of new DQ rules.  
**Review Frequency:** Monthly.