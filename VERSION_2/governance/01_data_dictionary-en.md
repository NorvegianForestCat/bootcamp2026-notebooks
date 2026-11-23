# Data Dictionary — Lakehouse Dota 2 (VERSION_2)

**Version:** 2.0 (Production)  
**Scope:** PostgreSQL `dota`, MongoDB `dota_events`, Iceberg layers RAW/STAGE/MART  
**Relevance:** This document describes **only the tables and fields that are actually used** in the implemented ETL pipeline (Case #3: Roshan & Victory).

---

## 1. DEFINITIONS

| Term | Definition |
|------|------------|
| **RAW** | Raw data layer, as close to the source as possible. Idempotent ingestion via `MERGE INTO` using watermarks. |
| **STAGE** | Layer of cleaned, typed, and deduplicated entities without business aggregations. |
| **MART** | Analytical marts with business logic, metrics, and features for specific use cases. |
| **PII** | Personally Identifiable Information (nicknames, account_id) — requires protection and minimization. |
| **DQ** | Data Quality — checks for completeness, correctness, consistency, and uniqueness. |

---

## 2. PostgreSQL — schema `dota`

### Table `matches` — Main matches table

| Field | PG Type | Iceberg Type | Description | PII | Required |
|-------|---------|--------------|-------------|-----|----------|
| `match_id` | BIGINT PK | LONG | Unique match ID (Valve) | No | Yes |
| `start_time` | INTEGER | INT | Unix timestamp of match start | No | Yes |
| `duration` | INTEGER | INT | Duration (seconds) | No | Yes |
| `radiant_win` | BOOLEAN | BOOLEAN | true if Radiant won | No | Yes |
| `human_players` | INTEGER | INT | Number of human players | No | Yes |
| `tower_status_radiant` | INTEGER | INT | Bitmask of Radiant towers | No | Yes |
| `tower_status_dire` | INTEGER | INT | Bitmask of Dire towers | No | Yes |
| `loaded_at` | TIMESTAMPTZ | TIMESTAMP | Insertion time into DB (used as watermark) | No | Yes |

**PK:** `match_id`  
**Validation:** `duration >= 600 AND duration <= 18000`, `human_players = 10`

---

### Table `match_players` — Player statistics in a match

| Field | PG Type | Iceberg Type | Description | PII | Required |
|-------|---------|--------------|-------------|-----|----------|
| `match_id` | BIGINT FK | LONG | Match ID (FK to `matches`) | No | Yes |
| `player_slot` | INTEGER | INT | Slot (0-127 Radiant, 128-255 Dire) | No | Yes |
| `account_id` | BIGINT | LONG | Steam account ID | **Yes (pseudonym)** | Yes |
| `hero_id` | INTEGER | INT | Hero ID (FK to `heroes`) | No | Yes |
| `is_radiant` | BOOLEAN | BOOLEAN | true if in Radiant | No | Yes |
| `win` | BOOLEAN | BOOLEAN | true if won the match | No | Yes |
| `loaded_at` | TIMESTAMPTZ | TIMESTAMP | Time of last row upsert | No | Yes |

**PK:** composite (`match_id`, `player_slot`)  
**Validation:** `player_slot IN (0,1,2,3,4,128,129,130,131,132)`, `hero_id > 0`

---

### Table `players` — Player names directory

| Field | PG Type | Iceberg Type | Description | PII | Required |
|-------|---------|--------------|-------------|-----|----------|
| `account_id` | BIGINT PK | LONG | Steam account ID | **Yes (pseudonym)** | Yes |
| `personaname` | TEXT | STRING | Display name in Steam | **Yes** | Yes |
| `loaded_at` | TIMESTAMPTZ | TIMESTAMP | Time of last row upsert | No | Yes |

**PK:** `account_id`  
**PII:** `personaname` — may contain toxic/personal data. Filtered: `account_id != 4294967295` (anonymous).

---

### Table `heroes` — Dota 2 Heroes directory

| Field | PG Type | Iceberg Type | Description | PII | Required |
|-------|---------|--------------|-------------|-----|----------|
| `hero_id` | INTEGER PK | INT | Unique hero ID (Valve) | No | Yes |
| `name` | TEXT | STRING | Internal name (e.g., `npc_dota_hero_antimage`) | No | Yes |
| `localized_name` | TEXT | STRING | Display name (e.g., `Anti-Mage`) | No | Yes |

**PK:** `hero_id`

---

### Table `npcs` — Stratz NPC directory

| Field | PG Type | Iceberg Type | Description | PII | Required |
|-------|---------|--------------|-------------|-----|----------|
| `npc_id` | INTEGER PK | INT | NPC ID | No | Yes |
| `name` | TEXT | STRING | Name (e.g., `npc_dota_roshan`) | No | Yes |
| `is_ancient` | BOOLEAN | BOOLEAN | Is ancient | No | Yes |

**PK:** `npc_id`  
**Usage:** `npc_id = 133` for filtering Roshan events.

---

## 3. MongoDB — database `dota_events`

### Common fields for all collections

| Field | Mongo Type | Iceberg Type | Description |
|-------|------------|--------------|-------------|
| `_id` | ObjectId | STRING | Unique document ID |
| `match_id` | long | LONG | Match ID |
| `created_at` | datetime | TIMESTAMP | Record time in UTC |

### Collection `cs_events` (Player-level)
Creep score events (last hits / denies), including Roshan.

| Field | Mongo Type | Iceberg Type | Description |
|-------|------------|--------------|-------------|
| `steam_account_id` | long | LONG | Steam account ID |
| `time` | int | INT | Event time (seconds) |
| `attacker` | int | INT | Player's steamAccountId |
| `npcId` | int | INT | Killed NPC ID (133 for Roshan) |
| `isFromIllusion` | bool | BOOLEAN | Killed by illusion |

---

### Collection `gold_events` (Player-level)
Gold acquisition.

| Field | Mongo Type | Iceberg Type | Description |
|-------|------------|--------------|-------------|
| `steam_account_id` | long | LONG | Steam account ID |
| `time` | int | INT | Acquisition time (seconds) |
| `amount` | int | INT | Amount of gold |
| `reason` | string | STRING | Reason: `ROSHAN`, `Kill`, `Tower`, etc. |

---

### Collection `tower_death_events` (Match-level)
Tower destruction moments.

| Field | Mongo Type | Iceberg Type | Description |
|-------|------------|--------------|-------------|
| `time` | int | INT | Destruction time (seconds) |
| `radiant` | int | INT | Bitmask of Radiant towers |
| `dire` | int | INT | Bitmask of Dire towers |

---

## 4. Table Relationships (Lineage)

```text
matches.match_id ← match_players.match_id
matches.match_id ← cs_events.match_id, gold_events.match_id, tower_death_events.match_id
match_players.account_id ← players.account_id
match_players.hero_id ← heroes.hero_id
cs_events.npcId ← npcs.npc_id (133 = Roshan)
```

**PII Flags:** `players.personaname`, `players.account_id`, `match_players.account_id` — pseudonymized, require protection during export.

---

## 5. Ownership & Maintenance

**Document Owner:** Data Engineering team.  
**Update Trigger:** Changes to source schemas or addition of new tables to the pipeline.  
**Review Frequency:** Monthly.