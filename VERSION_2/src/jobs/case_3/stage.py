import logging
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, LongType

from src.common.lakehouse_spark import SparkSessionContextManager

logging.basicConfig(
    level=logging.INFO,
    style="{",
    format="{asctime} [{levelname}] {message}"
)

funnel_records = []

LAKE_SOURCES = {
    "matches": "lakehouse.raw.pg__matches",
    "match_players": "lakehouse.raw.pg__match_players",
    "npcs": "lakehouse.raw.pg__npcs",
    "cs_events": "lakehouse.raw.mongo__cs_events",
    "gold_events": "lakehouse.raw.mongo__gold_events",
    "tower_death_events": "lakehouse.raw.mongo__tower_death_events",
}

LAKE_STAGES = {
    "matches": "lakehouse.stage.case03_pg__matches",
    "match_players": "lakehouse.stage.case03_pg__match_players",
    "cs_events": "lakehouse.stage.case03_mongo__cs_events",
    "gold_events": "lakehouse.stage.case03_mongo__gold_events",
    "tower_death_events": "lakehouse.stage.case03_mongo__tower_death_events",
}

def log_funnel(table_label: str, step_label: str, reason: str, before_cnt: int, after_cnt: int) -> None:
    """
    Logs data filtering funnel step metrics.

    Records the filtering step details including the table name, step label,
    reason for filtering, and counts before and after the filter was applied.
    Also calculates and logs the percentage of records removed.

    Args:
        table_label: Name of the table being filtered.
        step_label: Description of the filtering step/reason.
        reason: Text description of why records were removed.
        before_cnt: Record count before filtering.
        after_cnt: Record count after filtering.
    """
    removed = before_cnt - after_cnt
    pct = round((removed / before_cnt * 100), 2) if before_cnt > 0 else 0.0
    funnel_records.append({
        "Table": table_label,
        "Filtering Step": step_label,
        "Removal Reason": reason,
        "Before Count": before_cnt,
        "After Count": after_cnt,
        "Removed Count": removed,
        "Removed (%)": pct
    })
    logging.info(f"[{table_label}] {step_label}: {before_cnt} -> {after_cnt} (removed {removed}, {pct}%)")

def main() -> None:
    """
    Data cleaning and validation for Case 3 (Roshan).

    Executes the complete stage pipeline for Roshan case data processing,
    including deduplication, null checks, format filtering, and duration 
    validation for matches, players, and event tables.

    The pipeline writes cleaned/transformed DataFrames to the STAGE layer
    as Iceberg tables and prints a summary funnel report at the end.
    """
    with SparkSessionContextManager("case03-stage") as ctx:
        spark = ctx.spark

        # -------------------------------------------------------------------------
        # 1. Clean matches
        # -------------------------------------------------------------------------
        with ctx.lineage(
            "case03-stage-matches",
            inputs=[LAKE_SOURCES["matches"]],
            output=LAKE_STAGES["matches"],
            description="Deduplication and cleansing of matches (no business aggregations)"
        ):
            raw_m = spark.table(LAKE_SOURCES["matches"])
            c0 = raw_m.count()

            w_match = Window.partitionBy("match_id").orderBy(F.col("sys_insert_stamp").desc())
            m_dedup = raw_m.withColumn("rn", F.row_number().over(w_match)).filter("rn = 1").drop("rn")
            c1 = m_dedup.count()
            log_funnel("matches", "1. Deduplication", "Duplicate records by match_id", c0, c1)

            m_nulls = m_dedup.filter("match_id IS NOT NULL AND radiant_win IS NOT NULL AND duration IS NOT NULL")
            c2 = m_nulls.count()
            log_funnel("matches", "2. Null Check (NOT NULL)", "Missing match outcome or duration", c1, c2)

            m_humans = m_nulls.filter("human_players = 10")
            c3 = m_humans.count()
            log_funnel("matches", "3. 5v5 Match Format", "Incomplete lobbies or matches with bots", c2, c3)

            m_dur = m_humans.filter("duration >= 600 AND duration <= 18000")
            c4 = m_dur.count()
            log_funnel("matches", "4. Duration Filter", "Early leaves (< 10 min) or bugs (> 5 hrs)", c3, c4)

            clean_matches = m_dur.select(
                F.col("match_id").cast(LongType()).alias("match_id"),
                F.col("start_time").cast(LongType()).alias("start_time"),
                F.col("duration").cast(IntegerType()).alias("duration_sec"),
                F.col("radiant_win").cast("boolean").alias("radiant_win"),
                F.col("human_players").cast(IntegerType()).alias("human_players"),
                F.col("tower_status_radiant").cast(IntegerType()).alias("tower_status_radiant"),
                F.col("tower_status_dire").cast(IntegerType()).alias("tower_status_dire")
            )
            clean_matches.writeTo(LAKE_STAGES["matches"]).using("iceberg").createOrReplace()
            logging.info(f"Written: {LAKE_STAGES['matches']}")

        # -------------------------------------------------------------------------
        # 2. Clean match players
        # -------------------------------------------------------------------------
        with ctx.lineage(
            "case03-stage-players",
            inputs=[LAKE_SOURCES["match_players"], LAKE_STAGES["matches"]],
            output=LAKE_STAGES["match_players"],
            description="Deduplication and normalization of match players"
        ):
            raw_p = spark.table(LAKE_SOURCES["match_players"])
            p0 = raw_p.count()

            w_player = Window.partitionBy("match_id", "player_slot").orderBy(F.col("sys_insert_stamp").desc())
            p_dedup = raw_p.withColumn("rn", F.row_number().over(w_player)).filter("rn = 1").drop("rn")
            p1 = p_dedup.count()
            log_funnel("match_players", "1. Deduplication", "Duplicates by key (match_id, player_slot)", p0, p1)

            p_valid_slot = p_dedup.filter("player_slot IN (0, 1, 2, 3, 4, 128, 129, 130, 131, 132) AND hero_id > 0")
            p2 = p_valid_slot.count()
            log_funnel("match_players", "2. Validation of Slot and Hero", "Invalid player_slot or hero_id <= 0", p1, p2)

            valid_match_ids = spark.table(LAKE_STAGES["matches"]).select("match_id")
            p_in_clean_matches = p_valid_slot.join(valid_match_ids, on="match_id", how="inner")
            p3 = p_in_clean_matches.count()
            log_funnel("match_players", "3. Referential Integrity", "Players from filtered matches", p2, p3)

            clean_players = p_in_clean_matches.select(
                F.col("match_id").cast(LongType()).alias("match_id"),
                F.col("player_slot").cast(IntegerType()).alias("player_slot"),
                F.when(F.col("account_id") == 4294967295, F.lit(None)).otherwise(F.col("account_id").cast(LongType())).alias("account_id"),
                F.col("hero_id").cast(IntegerType()).alias("hero_id"),
                F.coalesce(F.col("is_radiant"), F.col("player_slot") < 128).cast("boolean").alias("is_radiant"),
                F.col("win").cast("boolean").alias("win")
            )
            clean_players.writeTo(LAKE_STAGES["match_players"]).using("iceberg").createOrReplace()
            logging.info(f"Written: {LAKE_STAGES['match_players']}")

        # -------------------------------------------------------------------------
        # 3. Clean Roshan kill events (cs_events)
        # -------------------------------------------------------------------------
        with ctx.lineage(
            "case03-stage-cs-events",
            inputs=[LAKE_SOURCES["cs_events"], LAKE_STAGES["matches"]],
            output=LAKE_STAGES["cs_events"],
            description="Cleaning and deduplication of Roshan kill events"
        ):
            raw_cs = spark.table(LAKE_SOURCES["cs_events"])
            cs0 = raw_cs.count()

            w_cs = Window.partitionBy("match_id", "id").orderBy(F.col("sys_insert_stamp").desc())
            cs_dedup = raw_cs.withColumn("rn", F.row_number().over(w_cs)).filter("rn = 1").drop("rn")
            cs1 = cs_dedup.count()
            log_funnel("cs_events", "1. Deduplication", "Duplicate documents by ObjectId", cs0, cs1)

            matches_bounds = spark.table(LAKE_STAGES["matches"]).select("match_id", "duration_sec")
            cs_in_matches = (
                cs_dedup.alias("e")
                .join(matches_bounds.alias("m"), "match_id", "inner")
                .filter(F.col("e.time").between(0, F.col("m.duration_sec")))
                .select("e.*").filter(F.col("npcId") == 133)
            )
            cs2 = cs_in_matches.count()
            log_funnel("cs_events", "2. Validation of Time and Match", "Events outside [0, duration] or from filtered matches", cs1, cs2)

            clean_cs = cs_in_matches.select(
                F.col("id").alias("event_id"),
                F.col("match_id").cast(LongType()).alias("match_id"),
                F.col("time").cast(IntegerType()).alias("time_sec"),
                F.col("steam_account_id").cast(LongType()).alias("steam_account_id"),
                F.col("attacker").cast(IntegerType()).alias("attacker_id"),
                F.col("npcId").cast(IntegerType()).alias("npc_id"),
                F.coalesce(F.col("isFromIllusion"), F.lit(False)).cast("boolean").alias("is_from_illusion")
            )
            clean_cs.writeTo(LAKE_STAGES["cs_events"]).using("iceberg").createOrReplace()
            logging.info(f"Written: {LAKE_STAGES['cs_events']}")

        # -------------------------------------------------------------------------
        # 4. Clean Roshan gold events
        # -------------------------------------------------------------------------
        with ctx.lineage(
            "case03-stage-gold-events",
            inputs=[LAKE_SOURCES["gold_events"], LAKE_STAGES["matches"]],
            output=LAKE_STAGES["gold_events"],
            description="Cleaning of Roshan kill events for gold allocation"
        ):
            matches_bounds = spark.table(LAKE_STAGES["matches"]).select("match_id", "duration_sec")

            raw_g = spark.table(LAKE_SOURCES["gold_events"]).filter(F.upper(F.col("reason")) == "ROSHAN")
            g0 = raw_g.count()
            w_g = Window.partitionBy("match_id", "id").orderBy(F.col("sys_insert_stamp").desc())
            g_dedup = raw_g.withColumn("rn", F.row_number().over(w_g)).filter("rn = 1 AND amount > 0").drop("rn")
            g1 = g_dedup.count()
            log_funnel("gold_events", "1. Deduplication and amount > 0", "Duplicate ObjectId or zero allocation", g0, g1)

            clean_gold = (
                g_dedup.join(matches_bounds, on="match_id", how="inner")
                .filter(F.col("time").between(0, F.col("duration_sec")))
                .select(
                    F.col("id").alias("event_id"),
                    F.col("match_id").cast(LongType()).alias("match_id"),
                    F.col("time").cast(IntegerType()).alias("time_sec"),
                    F.col("steam_account_id").cast(LongType()).alias("steam_account_id"),
                    F.col("amount").cast(IntegerType()).alias("gold_amount")
                )
            )
            g2 = clean_gold.count()
            log_funnel("gold_events", "2. Filter by valid matches", "Records outside match time", g1, g2)
            clean_gold.writeTo(LAKE_STAGES["gold_events"]).using("iceberg").createOrReplace()
            logging.info(f"Written: {LAKE_STAGES['gold_events']}")

        # -------------------------------------------------------------------------
        # 5. Clean tower death events
        # -------------------------------------------------------------------------
        with ctx.lineage(
            "case03-stage-tower-death-events",
            inputs=[LAKE_SOURCES["tower_death_events"], LAKE_STAGES["matches"]],
            output=LAKE_STAGES["tower_death_events"],
            description="Cleaning of tower death events"
        ):
            matches_bounds = spark.table(LAKE_STAGES["matches"]).select("match_id", "duration_sec")

            raw_t = spark.table(LAKE_SOURCES["tower_death_events"])
            t0 = raw_t.count()
            w_t = Window.partitionBy("match_id", "id").orderBy(F.col("sys_insert_stamp").desc())
            t_dedup = raw_t.withColumn("rn", F.row_number().over(w_t)).filter("rn = 1").drop("rn")
            t1 = t_dedup.count()
            log_funnel("tower_death_events", "1. Deduplication", "Duplicate documents by ObjectId", t0, t1)

            clean_towers = (
                t_dedup.join(matches_bounds, on="match_id", how="inner")
                .filter(F.col("time").between(0, F.col("duration_sec")))
                .select(
                    F.col("id").alias("event_id"),
                    F.col("match_id").cast(LongType()).alias("match_id"),
                    F.col("time").cast(IntegerType()).alias("time_sec"),
                    F.col("radiant").cast(IntegerType()).alias("radiant_tower_mask"),
                    F.col("dire").cast(IntegerType()).alias("dire_tower_mask")
                )
            )
            t2 = clean_towers.count()
            log_funnel("tower_death_events", "2. Filter by valid matches", "Events outside match time", t1, t2)
            clean_towers.writeTo(LAKE_STAGES["tower_death_events"]).using("iceberg").createOrReplace()
            logging.info(f"Written: {LAKE_STAGES['tower_death_events']}")

        # Print final funnel report
        print("\n=== DATA CLEANING FUNNEL (STAGE) ===")
        funnel_df = pd.DataFrame(funnel_records)
        print(funnel_df.to_string(index=False))

if __name__ == "__main__":
    main()