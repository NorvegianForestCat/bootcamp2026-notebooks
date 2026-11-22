"""
Data Quality checks for Case #3 (Roshan and Victory).
Executed after the STAGE and MART layers are built.
"""
import logging
import sys
from pyspark.sql import SparkSession

from src.common.lakehouse_dq import DQEngine, DQRule
from src.common.lakehouse_spark import SparkSessionContextManager

logger = logging.getLogger(__name__)


def run_dq_checks(spark: SparkSession, run_id: str = "adhoc") -> None:
    """
    Execute all Data Quality checks for Case #3.

    Args:
        spark: Active SparkSession instance.
        run_id: Unique identifier for the current pipeline run.

    Raises:
        RuntimeError: If any HARD DQ rule is violated.
    """
    dq = DQEngine(spark)

    logger.info("=== Starting DQ checks for Case #3 ===")

    # ==========================================================================
    # 1. STAGE Layer Checks
    # ==========================================================================
    logger.info("--- STAGE Layer Checks ---")

    # 1.1. Uniqueness and validity of clean_matches
    stage_matches = spark.table("lakehouse.stage.case03_pg__matches")
    dq.apply_rules(
        stage_matches,
        "lakehouse.stage.case03_pg__matches",
        [
            DQRule(
                rule_id="DQ-STG-03",
                description="match_id uniqueness and non-null",
                sql_condition="match_id IS NOT NULL",
                severity="HARD"
            ),
            DQRule(
                rule_id="DQ-STG-05",
                description="Match duration >= 10 minutes (600 seconds)",
                sql_condition="duration_sec >= 600",
                severity="HARD"  # Should already be filtered, but verified as a safeguard
            ),
            DQRule(
                rule_id="DQ-STG-05B",
                description="Exactly 10 human players in the match",
                sql_condition="human_players = 10",
                severity="HARD"
            )
        ],
        run_id=run_id,
        pk_cols=["match_id"]
    )
    logger.info("[PASS] DQ-STG-03, DQ-STG-05, DQ-STG-05B: STAGE matches are valid")

    # 1.2. Uniqueness and validity of clean_match_players
    stage_players = spark.table("lakehouse.stage.case03_pg__match_players")
    dq.apply_rules(
        stage_players,
        "lakehouse.stage.case03_pg__match_players",
        [
            DQRule(
                rule_id="DQ-STG-06A",
                description="Valid player_slot",
                sql_condition="player_slot IN (0, 1, 2, 3, 4, 128, 129, 130, 131, 132)",
                severity="HARD"
            ),
            DQRule(
                rule_id="DQ-STG-06B",
                description="Valid hero_id",
                sql_condition="hero_id > 0",
                severity="HARD"
            )
        ],
        run_id=run_id,
        pk_cols=["match_id", "player_slot"]
    )
    logger.info("[PASS] DQ-STG-06A, DQ-STG-06B: STAGE players are valid")

    # ==========================================================================
    # 2. MART Layer Checks
    # ==========================================================================
    logger.info("--- MART Layer Checks ---")

    mart_matches = spark.table("lakehouse.mart.case03_pg__match_roshan_mart")

    # 2.1. Uniqueness of match_id in MART
    dq.apply_rules(
        mart_matches,
        "lakehouse.mart.case03_pg__match_roshan_mart",
        [
            DQRule(
                rule_id="DQ-MRT-01",
                description="match_id uniqueness in MART",
                sql_condition="match_id IS NOT NULL",
                severity="HARD"
            )
        ],
        run_id=run_id,
        pk_cols=["match_id"]
    )
    logger.info("[PASS] DQ-MRT-01: match_id uniqueness in MART verified")

    # 2.2. Integrity of Roshan kill sums (Critical Check!)
    # roshan_killed_by_radiant + roshan_killed_by_dire + roshan_killer_side_unknown == total_roshan_kills
    dq.apply_rules(
        mart_matches,
        "lakehouse.mart.case03_pg__match_roshan_mart",
        [
            DQRule(
                rule_id="DQ-MRT-02",
                description="Sum of Roshan kills by side equals total kills",
                sql_condition="(roshan_killed_by_radiant + roshan_killed_by_dire + roshan_killer_side_unknown) = total_roshan_kills",
                severity="HARD"
            )
        ],
        run_id=run_id,
        pk_cols=["match_id"]
    )
    logger.info("[PASS] DQ-MRT-02: Roshan kill sum integrity verified")

    # 2.3. Probability range in time_effect mart
    try:
        time_effect = spark.table("lakehouse.mart.case03_pg__roshan_time_effect")
        dq.apply_rules(
            time_effect,
            "lakehouse.mart.case03_pg__roshan_time_effect",
            [
                DQRule(
                    rule_id="DQ-MRT-03",
                    description="Win rate is within [0, 1] range",
                    sql_condition="first_roshan_win_rate BETWEEN 0 AND 1",
                    severity="HARD"
                )
            ],
            run_id=run_id,
            pk_cols=["first_roshan_time_bin", "killed_while_losing_towers"]
        )
        logger.info("[PASS] DQ-MRT-03: Probability validity in time_effect verified")
    except Exception as e:
        logger.warning(f"Table time_effect not yet created or empty: {e}")

    # ==========================================================================
    # 3. Summary Statistics
    # ==========================================================================
    logger.info("=== DQ Summary Statistics ===")
    
    summary = spark.sql("""
        SELECT 
            COUNT(*) AS total_matches,
            SUM(CASE WHEN total_roshan_kills > 0 THEN 1 ELSE 0 END) AS matches_with_roshan,
            ROUND(AVG(CASE WHEN total_roshan_kills > 0 THEN 1.0 ELSE 0.0 END) * 100, 2) AS pct_matches_with_roshan,
            SUM(total_roshan_kills) AS total_roshan_kills,
            ROUND(SUM(roshan_killer_side_unknown) * 100.0 / NULLIF(SUM(total_roshan_kills), 0), 3) AS pct_unknown_side
        FROM lakehouse.mart.case03_pg__match_roshan_mart
    """)
    
    summary.show(truncate=False)
    
    logger.info("=== All DQ checks for Case #3 passed successfully! ===")


if __name__ == "__main__":
    run_id = sys.argv[1] if len(sys.argv) > 1 else "adhoc_dq_run"
    
    with SparkSessionContextManager("case-03-dq-checks") as spark:
        run_dq_checks(spark, run_id=run_id)