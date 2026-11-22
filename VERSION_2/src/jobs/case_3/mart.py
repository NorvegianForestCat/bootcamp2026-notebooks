import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.lakehouse_spark import SparkSessionContextManager

logging.basicConfig(
    level=logging.INFO,
    style="{",
    format="{asctime} [{levelname}] {message}"
)

LAKE_STAGES = {
    "matches": "lakehouse.stage.case03_pg__matches",
    "match_players": "lakehouse.stage.case03_pg__match_players",
    "cs_events": "lakehouse.stage.case03_mongo__cs_events",
    "gold_events": "lakehouse.stage.case03_mongo__gold_events",
    "tower_death_events": "lakehouse.stage.case03_mongo__tower_death_events",
}

LAKE_MARTS = {
    "match_roshan": "lakehouse.mart.case03_pg__match_roshan_mart",
    "time_effect": "lakehouse.mart.case03_pg__roshan_time_effect",
    "control_effect": "lakehouse.mart.case03_pg__roshan_control_effect",
}

def main() -> None:
    """
    Builds analytical MART tables for Case 3 (Roshan).

    Executes the complete MART pipeline for Roshan case data processing,
    including:
    1. Match-level Roshan mart: side resolution, tower status before kill, 
       match pace, and detailed Roshan metrics.
    2. Time effect mart: aggregated win rates, quantiles, and gold by 
       first Roshan time intervals.
    3. Control effect mart: aggregated win rates by Roshan control status, 
       lead magnitude, and match pace.

    The pipeline reads from the STAGE layer, applies business logic, 
    and writes the resulting DataFrames to the MART layer as Iceberg tables.
    """
    with SparkSessionContextManager("case03-mart") as ctx:
        spark = ctx.spark

        # -------------------------------------------------------------------------
        # 1. Detailed match-level business mart
        # -------------------------------------------------------------------------
        with ctx.lineage(
            "case03-mart-match-roshan",
            inputs=[
                LAKE_STAGES["matches"],
                LAKE_STAGES["match_players"],
                LAKE_STAGES["cs_events"],
                LAKE_STAGES["gold_events"],
                LAKE_STAGES["tower_death_events"]
            ],
            output=LAKE_MARTS["match_roshan"],
            description="Full business logic: side resolution, tower status, pace, and metrics"
        ):
            matches_df = spark.table(LAKE_STAGES["matches"])
            players_df = spark.table(LAKE_STAGES["match_players"])
            cs_df = spark.table(LAKE_STAGES["cs_events"])
            gold_df = spark.table(LAKE_STAGES["gold_events"])
            towers_df = spark.table(LAKE_STAGES["tower_death_events"])

            # Player mapping
            players_by_acc = (
                players_df.filter("account_id IS NOT NULL")
                .select("match_id", "account_id", F.col("is_radiant").alias("acc_is_radiant"))
                .dropDuplicates(["match_id", "account_id"])
            )
            players_by_hero = (
                players_df.select("match_id", "hero_id", F.col("is_radiant").alias("hero_is_radiant"))
                .dropDuplicates(["match_id", "hero_id"])
            )

            # Aggregate team gold for Roshan in 5-second windows
            gold_buckets = (
                gold_df.alias("g")
                .join(players_by_acc.alias("pa"), 
                      (F.col("g.match_id") == F.col("pa.match_id")) & (F.col("g.steam_account_id") == F.col("pa.account_id")), "left")
                .withColumn("time_bucket", F.floor(F.col("g.time_sec") / 5) * 5)
                .groupBy(F.col("g.match_id").alias("g_match_id"), "time_bucket")
                .agg(
                    F.first("pa.acc_is_radiant", ignorenulls=True).alias("gold_is_radiant"),
                    F.sum("g.gold_amount").alias("roshan_team_gold")
                )
            )

            # Cascading side resolution for Roshan killer
            roshan_with_side = (
                cs_df.alias("r")
                .join(players_by_acc.alias("pa"), 
                      (F.col("r.match_id") == F.col("pa.match_id")) & (F.col("r.steam_account_id") == F.col("pa.account_id")), "left")
                .join(players_by_hero.alias("ph"), 
                      (F.col("r.match_id") == F.col("ph.match_id")) & (F.col("r.attacker_id") == F.col("ph.hero_id")), "left")
                .join(gold_buckets.alias("gb"), 
                      (F.col("r.match_id") == F.col("gb.g_match_id")) & (F.floor(F.col("r.time_sec") / 5) * 5 == F.col("gb.time_bucket")), "left")
                .select(
                    F.col("r.match_id"),
                    F.col("r.event_id"),
                    F.col("r.time_sec").alias("roshan_time"),
                    F.coalesce(F.col("pa.acc_is_radiant"), F.col("ph.hero_is_radiant"), F.col("gb.gold_is_radiant")).alias("killer_is_radiant"),
                    F.coalesce(F.col("gb.roshan_team_gold"), F.lit(0)).alias("roshan_team_gold"),
                    F.when(F.col("pa.acc_is_radiant").isNotNull(), "1_account_id")
                     .when(F.col("ph.hero_is_radiant").isNotNull(), "2_hero_id")
                     .when(F.col("gb.gold_is_radiant").isNotNull(), "3_gold_events")
                     .otherwise("4_unknown").alias("side_resolution_method")
                )
            )

            # Tower status strictly BEFORE the Roshan kill
            towers_alive = (
                towers_df
                .withColumn("radiant_towers_alive", F.bit_count(F.col("radiant_tower_mask").bitwiseAND(F.lit(2047))))
                .withColumn("dire_towers_alive", F.bit_count(F.col("dire_tower_mask").bitwiseAND(F.lit(2047))))
                .select(F.col("match_id").alias("t_match_id"), F.col("time_sec").alias("tower_time"), "radiant_towers_alive", "dire_towers_alive")
            )

            w_last_tower = Window.partitionBy("match_id", "event_id").orderBy(F.col("tower_time").desc())
            w_roshan_seq = Window.partitionBy("match_id").orderBy(F.col("roshan_time").asc(), F.col("event_id").asc())

            roshan_events_enriched = (
                roshan_with_side
                .join(towers_alive, 
                      (roshan_with_side["match_id"] == towers_alive["t_match_id"]) & (towers_alive["tower_time"] < roshan_with_side["roshan_time"]), "left")
                .withColumn("rn", F.row_number().over(w_last_tower))
                .filter("rn = 1")
                .drop("rn", "t_match_id", "tower_time")
                .fillna({"radiant_towers_alive": 11, "dire_towers_alive": 11})
                .withColumn("roshan_kill_num", F.row_number().over(w_roshan_seq))
                .withColumn("prev_roshan_time", F.lag("roshan_time").over(w_roshan_seq))
                .withColumn("respawn_interval_sec", F.col("roshan_time") - F.col("prev_roshan_time"))
                .withColumn(
                    "tower_diff_at_kill",
                    F.when(F.col("killer_is_radiant") == True, F.col("radiant_towers_alive") - F.col("dire_towers_alive"))
                     .when(F.col("killer_is_radiant") == False, F.col("dire_towers_alive") - F.col("radiant_towers_alive"))
                     .otherwise(F.lit(None))
                )
                .withColumn("killed_while_losing_towers", F.col("tower_diff_at_kill") < 0)
            )

            first_roshan = (
                roshan_events_enriched.filter("roshan_kill_num = 1")
                .select(
                    "match_id",
                    F.col("roshan_time").alias("first_roshan_time"),
                    F.round(F.col("roshan_time") / 60.0, 2).alias("first_roshan_time_min"),
                    F.col("killer_is_radiant").alias("first_roshan_is_radiant"),
                    F.col("killed_while_losing_towers").alias("first_roshan_while_losing_towers"),
                    F.col("tower_diff_at_kill").alias("first_roshan_tower_diff"),
                    F.col("roshan_team_gold").alias("first_roshan_gold")
                )
            )

            roshan_agg = roshan_events_enriched.groupBy("match_id").agg(
                F.count("*").alias("total_roshan_kills"),
                F.max("roshan_time").alias("last_roshan_time"),
                F.avg("respawn_interval_sec").alias("avg_roshan_interval_sec"),
                F.sum("roshan_team_gold").alias("total_roshan_gold"),
                F.sum(F.when(F.col("killer_is_radiant") == True, 1).otherwise(0)).alias("roshan_killed_by_radiant"),
                F.sum(F.when(F.col("killer_is_radiant") == False, 1).otherwise(0)).alias("roshan_killed_by_dire"),
                F.sum(F.when(F.col("killer_is_radiant").isNull(), 1).otherwise(0)).alias("roshan_killer_side_unknown"),
                F.sum(F.when((F.col("roshan_time") < 900) & (F.col("killer_is_radiant") == True), 1).otherwise(0)).alias("early_15m_radiant"),
                F.sum(F.when((F.col("roshan_time") < 900) & (F.col("killer_is_radiant") == False), 1).otherwise(0)).alias("early_15m_dire"),
                F.sum(F.when(F.col("killed_while_losing_towers") == True, 1).otherwise(0)).alias("roshan_kills_while_losing_towers")
            )

            match_roshan_mart = (
                matches_df.alias("m")
                .join(roshan_agg.alias("a"), "match_id", "left")
                .join(first_roshan.alias("f"), "match_id", "left")
                .select(
                    "m.match_id",
                    "m.radiant_win",
                    "m.duration_sec",
                    F.round(F.col("m.duration_sec") / 60.0, 2).alias("duration_min"),
                    F.when(F.col("m.duration_sec") < 1500, "1_Fast (<25 min)")
                     .when(F.col("m.duration_sec") < 2400, "2_Medium (25-40 min)")
                     .when(F.col("m.duration_sec") < 3300, "3_Long (40-55 min)")
                     .otherwise("4_Very Long (55+ min)").alias("match_pace_bin"),
                    F.coalesce("a.total_roshan_kills", F.lit(0)).alias("total_roshan_kills"),
                    F.coalesce("a.roshan_killed_by_radiant", F.lit(0)).alias("roshan_killed_by_radiant"),
                    F.coalesce("a.roshan_killed_by_dire", F.lit(0)).alias("roshan_killed_by_dire"),
                    F.coalesce("a.roshan_killer_side_unknown", F.lit(0)).alias("roshan_killer_side_unknown"),
                    (F.coalesce("a.roshan_killed_by_radiant", F.lit(0)) - F.coalesce("a.roshan_killed_by_dire", F.lit(0))).alias("roshan_diff_radiant"),
                    F.abs(F.coalesce("a.roshan_killed_by_radiant", F.lit(0)) - F.coalesce("a.roshan_killed_by_dire", F.lit(0))).alias("roshan_lead_abs"),
                    F.coalesce("a.total_roshan_gold", F.lit(0)).alias("total_roshan_gold"),
                    F.round(F.col("a.avg_roshan_interval_sec") / 60.0, 2).alias("avg_roshan_interval_min"),
                    F.coalesce("a.roshan_kills_while_losing_towers", F.lit(0)).alias("roshan_kills_while_losing_towers"),
                    "f.first_roshan_while_losing_towers",
                    "f.first_roshan_tower_diff",
                    "f.first_roshan_time",
                    "f.first_roshan_time_min",
                    "f.first_roshan_is_radiant",
                    "f.first_roshan_gold",
                    F.when(F.col("f.first_roshan_is_radiant").isNull(), F.lit(None))
                     .otherwise(F.col("f.first_roshan_is_radiant") == F.col("m.radiant_win")).alias("first_roshan_team_won"),
                    F.when(F.col("f.first_roshan_time").isNull(), "0_No_Roshan")
                     .when(F.col("f.first_roshan_time") < 900, "1_0-15_min")
                     .when(F.col("f.first_roshan_time") < 1500, "2_15-25_min")
                     .when(F.col("f.first_roshan_time") < 2100, "3_25-35_min")
                     .when(F.col("f.first_roshan_time") < 2700, "4_35-45_min")
                     .otherwise("5_45+_min").alias("first_roshan_time_bin"),
                    F.when(F.coalesce("a.total_roshan_kills", F.lit(0)) == 0, "No_Roshans")
                     .when(F.coalesce("a.roshan_killed_by_radiant", F.lit(0)) > F.coalesce("a.roshan_killed_by_dire", F.lit(0)), "Radiant_Lead")
                     .when(F.coalesce("a.roshan_killed_by_dire", F.lit(0)) > F.coalesce("a.roshan_killed_by_radiant", F.lit(0)), "Dire_Lead")
                     .otherwise("Roshan_Tie").alias("roshan_control_status")
                )
            )

            match_roshan_mart.writeTo(LAKE_MARTS["match_roshan"]).using("iceberg").createOrReplace()
            logging.info(f"Written: {LAKE_MARTS['match_roshan']}")

        # -------------------------------------------------------------------------
        # 2. Aggregated mart: First Roshan time effect
        # -------------------------------------------------------------------------
        with ctx.lineage(
            "case03-mart-time-effect",
            inputs=[LAKE_MARTS["match_roshan"]],
            output=LAKE_MARTS["time_effect"],
            description="Aggregated win rates, time quantiles, duration, and gold by first Roshan intervals"
        ):
            time_effect = spark.sql("""
                select
                    first_roshan_time_bin,
                    first_roshan_while_losing_towers as killed_while_losing_towers,
                    count(*) as matches,
                    round(count(*) * 100.0 / sum(count(*)) over (), 2) as share_of_roshan_matches_pct,
                    sum(case when first_roshan_team_won then 1 else 0 end) as wins,
                    avg(case when first_roshan_team_won then 1.0 else 0.0 end) as first_roshan_win_rate,
                    avg(case when first_roshan_team_won then 1.0 else 0.0 end) - 0.5 as lift,
                    round(avg(first_roshan_time_min), 2) as mean_roshan_time_min,
                    round(percentile_approx(first_roshan_time_min, 0.25), 2) as p25_roshan_time_min,
                    round(percentile_approx(first_roshan_time_min, 0.50), 2) as median_roshan_time_min,
                    round(percentile_approx(first_roshan_time_min, 0.75), 2) as p75_roshan_time_min,
                    round(avg(duration_min), 2) as avg_match_duration_min,
                    round(avg(total_roshan_kills), 2) as avg_roshans_per_match,
                    round(avg(first_roshan_gold), 0) as avg_first_roshan_gold
                from lakehouse.mart.practice_case_03_match_roshan_mart
                where first_roshan_time_bin <> '0_No_Roshan'
                  and first_roshan_team_won is not null
                group by 1, 2
            """)
            time_effect.writeTo(LAKE_MARTS["time_effect"]).using("iceberg").createOrReplace()
            logging.info(f"Written: {LAKE_MARTS['time_effect']}")

        # -------------------------------------------------------------------------
        # 3. Aggregated mart: Roshan control and match pace
        # -------------------------------------------------------------------------
        with ctx.lineage(
            "case03-mart-control-effect",
            inputs=[LAKE_MARTS["match_roshan"]],
            output=LAKE_MARTS["control_effect"],
            description="Aggregated win rates by Roshan control status, lead magnitude, and match pace"
        ):
            control_effect = spark.sql("""
                select
                    match_pace_bin,
                    roshan_control_status,
                    roshan_lead_abs,
                    count(*) as matches,
                    round(avg(duration_min), 2) as avg_duration_min,
                    round(percentile_approx(duration_min, 0.50), 2) as median_duration_min,
                    round(avg(total_roshan_kills), 2) as avg_total_roshan_kills,
                    round(avg(avg_roshan_interval_min), 2) as avg_respawn_interval_min,
                    round(avg(total_roshan_gold), 0) as avg_total_roshan_gold,
                    round(avg(case when first_roshan_while_losing_towers then 1.0 else 0.0 end) * 100, 2) as losing_towers_share_pct,
                    avg(case
                        when roshan_control_status = 'Radiant_Lead' and radiant_win then 1.0
                        when roshan_control_status = 'Dire_Lead' and not radiant_win then 1.0
                        when roshan_control_status in ('Radiant_Lead', 'Dire_Lead') then 0.0
                        else null
                    end) as leader_win_rate
                from lakehouse.mart.practice_case_03_match_roshan_mart
                group by 1, 2, 3
            """)
            control_effect.writeTo(LAKE_MARTS["control_effect"]).using("iceberg").createOrReplace()
            logging.info(f"Written: {LAKE_MARTS['control_effect']}")

        print("\n=== MART tables successfully created ===")

if __name__ == "__main__":
    main()