"""
Data Quality проверки для Кейса №3 (Рошан и победа).
Запускается после построения STAGE и MART слоёв.
"""
import logging
from pyspark.sql import SparkSession
from src.common.lakehouse_dq import DQEngine, DQRule
from src.common.lakehouse_spark import SparkSessionContext

logger = logging.getLogger(__name__)


def run_dq_checks(spark: SparkSession, run_id: str = "adhoc"):
    """
    Выполняет все DQ-проверки для кейса 3.
    """
    dq = DQEngine(spark)

    logger.info("=== Запуск DQ-проверок для Кейса №3 ===")

    # ==========================================================================
    # 1. Проверки STAGE слоя
    # ==========================================================================
    logger.info("--- Проверки STAGE слоя ---")

    # 1.1. Уникальность match_id в clean_matches
    stage_matches = spark.table("lakehouse.stage.practice_case_03_clean_matches")
    dq.apply_rules(
        stage_matches,
        "stage.practice_case_03_clean_matches",
        [
            DQRule(
                rule_id="DQ-STG-03",
                description="Уникальность match_id",
                sql_condition="match_id IS NOT NULL",
                severity="HARD"
            ),
            DQRule(
                rule_id="DQ-STG-05",
                description="Длительность матча >= 10 минут (600 сек)",
                sql_condition="duration_sec >= 600",
                severity="HARD"  # Должно быть уже отфильтровано, но проверяем на всякий случай
            ),
            DQRule(
                rule_id="DQ-STG-05B",
                description="Ровно 10 игроков в матче",
                sql_condition="human_players = 10",
                severity="HARD"
            )
        ],
        run_id=run_id,
        pk_cols=["match_id"]
    )
    logger.info("[PASS] DQ-STG-03, DQ-STG-05, DQ-STG-05B: Матчи STAGE валидны")

    # 1.2. Уникальность и валидность match_players
    stage_players = spark.table("lakehouse.stage.practice_case_03_clean_match_players")
    dq.apply_rules(
        stage_players,
        "stage.practice_case_03_clean_match_players",
        [
            DQRule(
                rule_id="DQ-STG-06A",
                description="Валидный player_slot",
                sql_condition="player_slot IN (0, 1, 2, 3, 4, 128, 129, 130, 131, 132)",
                severity="HARD"
            ),
            DQRule(
                rule_id="DQ-STG-06B",
                description="Валидный hero_id",
                sql_condition="hero_id > 0",
                severity="HARD"
            )
        ],
        run_id=run_id,
        pk_cols=["match_id", "player_slot"]
    )
    logger.info("[PASS] DQ-STG-06A, DQ-STG-06B: Игроки STAGE валидны")

    # ==========================================================================
    # 2. Проверки MART слоя
    # ==========================================================================
    logger.info("--- Проверки MART слоя ---")

    mart_matches = spark.table("lakehouse.mart.practice_case_03_match_roshan_mart")

    # 2.1. Уникальность match_id в MART
    dq.apply_rules(
        mart_matches,
        "mart.practice_case_03_match_roshan_mart",
        [
            DQRule(
                rule_id="DQ-MRT-01",
                description="Уникальность match_id в MART",
                sql_condition="match_id IS NOT NULL",
                severity="HARD"
            )
        ],
        run_id=run_id,
        pk_cols=["match_id"]
    )
    logger.info("[PASS] DQ-MRT-01: Уникальность match_id в MART")

    # 2.2. Целостность сумм убийств Рошана (Ключевая проверка!)
    # roshan_killed_by_radiant + roshan_killed_by_dire + roshan_killer_side_unknown == total_roshan_kills
    dq.apply_rules(
        mart_matches,
        "mart.practice_case_03_match_roshan_mart",
        [
            DQRule(
                rule_id="DQ-MRT-02",
                description="Сумма убийств Рошана по сторонам равна общему количеству",
                sql_condition="(roshan_killed_by_radiant + roshan_killed_by_dire + roshan_killer_side_unknown) = total_roshan_kills",
                severity="HARD"
            )
        ],
        run_id=run_id,
        pk_cols=["match_id"]
    )
    logger.info("[PASS] DQ-MRT-02: Целостность сумм убийств Рошана")

    # 2.3. Диапазон вероятностей в витрине time_effect
    try:
        time_effect = spark.table("lakehouse.mart.practice_case_03_roshan_time_effect")
        dq.apply_rules(
            time_effect,
            "mart.practice_case_03_roshan_time_effect",
            [
                DQRule(
                    rule_id="DQ-MRT-03",
                    description="Винрейт в диапазоне [0, 1]",
                    sql_condition="first_roshan_win_rate BETWEEN 0 AND 1",
                    severity="HARD"
                )
            ],
            run_id=run_id,
            pk_cols=["first_roshan_time_bin", "killed_while_losing_towers"]
        )
        logger.info("[PASS] DQ-MRT-03: Валидность вероятностей в time_effect")
    except Exception as e:
        logger.warning(f"Таблица time_effect ещё не создана или пуста: {e}")

    # ==========================================================================
    # 3. Сводная статистика по качеству
    # ==========================================================================
    logger.info("=== Сводная статистика DQ ===")
    
    summary = spark.sql("""
        SELECT 
            COUNT(*) AS total_matches,
            SUM(CASE WHEN total_roshan_kills > 0 THEN 1 ELSE 0 END) AS matches_with_roshan,
            ROUND(AVG(CASE WHEN total_roshan_kills > 0 THEN 1.0 ELSE 0.0 END) * 100, 2) AS pct_matches_with_roshan,
            SUM(total_roshan_kills) AS total_roshan_kills,
            ROUND(SUM(roshan_killer_side_unknown) * 100.0 / NULLIF(SUM(total_roshan_kills), 0), 3) AS pct_unknown_side
        FROM lakehouse.mart.practice_case_03_match_roshan_mart
    """)
    
    summary.show(truncate=False)
    
    logger.info("=== Все DQ-проверки для Кейса №3 успешно пройдены! ===")


if __name__ == "__main__":
    import sys
    run_id = sys.argv[1] if len(sys.argv) > 1 else "adhoc_dq_run"
    
    with SparkSessionContext("case-03-dq-checks") as spark:
        run_dq_checks(spark, run_id=run_id)