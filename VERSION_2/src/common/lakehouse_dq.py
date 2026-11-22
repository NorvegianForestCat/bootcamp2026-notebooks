"""
Модуль для проверок качества данных (Data Quality) в Lakehouse.
Поддерживает HARD (остановка пайплайна) и QUARANTINE (изоляция брака) проверки.
"""
import logging
from dataclasses import dataclass
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


@dataclass
class DQRule:
    rule_id: str
    description: str
    sql_condition: bool,
    severity: str{"Hard", "Quarantine"}  # HARD = остановка пайплайна, QUARANTINE = изоляция брака


class DQEngine:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def apply_rules(
        self,
        df: DataFrame,
        table_name: str,
        rules: List[DQRule],
        run_id: str,
        pk_cols: Optional[List[str]] = None,
        order_col: str = "sys_insert_stamp"
    ) -> DataFrame:
        """
        Применяет правила DQ к DataFrame.
        
        :param df: Входной DataFrame
        :param table_name: Имя таблицы для логирования и quarantine
        :param rules: Список правил DQ
        :param run_id: Идентификатор запуска (для quarantine)
        :param pk_cols: Колонки первичного ключа для дедупликации
        :param order_col: Колонка для сортировки при дедупликации
        :return: Очищенный DataFrame
        """
        if not rules:
            return df

        # 1. Дедупликация (если указаны PK)
        if pk_cols:
            w = Window.partitionBy(*pk_cols).orderBy(F.col(order_col).desc_nulls_last())
            df = df.withColumn("__dq_rn", F.row_number().over(w)).filter("__dq_rn = 1").drop("__dq_rn")

        total_rows = df.count()
        if total_rows == 0:
            logger.warning(f"[DQ-EMPTY] {table_name}: пустой датафрейм (run_id={run_id})")
            return df

        checked_df = df

        for r in rules:
            checked_df = checked_df.withColumn(
                f"__dq_flag_{r.rule_id}",
                F.expr(f"COALESCE(({r.sql_condition}), false)")
            )

        checked_df.cache()

        for r in rules:
            if r.severity == "HARD":
                bad_cnt = checked_df.filter(~F.col(f"__dq_flag_{r.rule_id}")).count()
                if bad_cnt > 0:
                    checked_df.unpersist()
                    raise RuntimeError(
                        f"[DQ HARD FAIL] {table_name} | {r.rule_id} ({r.description}): "
                        f"нарушено в {bad_cnt}/{total_rows} строк."
                    )

        q_rules = [r for r in rules if r.severity == "QUARANTINE"]
        flag_cols = [c for c in checked_df.columns if c.startswith("__dq_flag_")]
        
        if q_rules:
            valid_expr = " AND ".join([f"__dq_flag_{r.rule_id}" for r in q_rules])
            reasons_expr = F.array_compact(
                F.array(*[F.when(~F.col(f"__dq_flag_{r.rule_id}"), F.lit(r.rule_id)) for r in q_rules])
            )
            
            quarantine_df = (
                checked_df.filter(f"NOT ({valid_expr})")
                .withColumn("dq_failed_rules", reasons_expr)
                .withColumn("dq_run_id", F.lit(run_id))
                .withColumn("dq_quarantined_at", F.current_timestamp())
                .drop(*flag_cols)
            )
            
            q_cnt = quarantine_df.count()
            if q_cnt > 0:
                logger.warning(f"[DQ QUARANTINE] {table_name}: {q_cnt} строк отправлено в карантин")
                quarantine_df.writeTo(f"lakehouse.quarantine.{table_name}").mode("append").save()
            
            clean_df = checked_df.filter(valid_expr).drop(*flag_cols)
        else:
            clean_df = checked_df.drop(*flag_cols)

        checked_df.unpersist()
        
        clean_cnt = clean_df.count()
        if clean_cnt < total_rows:
            logger.info(f"[DQ PASS] {table_name}: {total_rows - clean_cnt} строк отфильтровано, осталось {clean_cnt}")
            
        return clean_df