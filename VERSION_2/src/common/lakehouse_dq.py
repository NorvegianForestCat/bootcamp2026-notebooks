"""
Data Quality validation module for the Lakehouse architecture.
Supports HARD (pipeline halt) and QUARANTINE (bad record isolation) checks.
"""
import logging
from dataclasses import dataclass
from typing import List, Optional, Literal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


@dataclass
class DQRule:
    """
    Represents a single Data Quality rule.

    Attributes:
        rule_id: Unique identifier for the rule (e.g., 'DQ-STG-01').
        description: Human-readable description of what the rule checks.
        sql_condition: SQL expression that must evaluate to True for valid rows.
        severity: 'HARD' to fail the pipeline, 'QUARANTINE' to isolate bad rows.
    """
    rule_id: str
    description: str
    sql_condition: str
    severity: Literal["HARD", "QUARANTINE"]


class DQEngine:
    """
    Engine for applying Data Quality rules to Spark DataFrames.

    Provides deduplication, HARD assertion checks, and QUARANTINE isolation
    for rows that fail validation.
    """
    def __init__(self, spark: SparkSession):
        """
        Initialize the DQEngine.

        Args:
            spark: Active SparkSession instance.
        """
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
        Apply DQ rules to a DataFrame.

        Args:
            df: Input DataFrame to validate.
            table_name: Target table name for logging and quarantine routing.
            rules: List of DQRule objects to apply.
            run_id: Unique identifier for the current pipeline run.
            pk_cols: List of columns forming the primary key for deduplication.
            order_col: Column to order by when deduplicating (default: 'sys_insert_stamp').

        Returns:
            DataFrame: Cleaned DataFrame with invalid rows removed or quarantined.

        Raises:
            RuntimeError: If a HARD rule is violated.
        """
        if not rules:
            return df
        
        # 1. Deduplication (if PK is specified)
        if pk_cols:
            order_expr = F.col(order_col).desc_nulls_last() if order_col in df.columns else F.lit(1)
            w = Window.partitionBy(*pk_cols).orderBy(order_expr)
            df = df.withColumn("__dq_rn", F.row_number().over(w)).filter("__dq_rn = 1").drop("__dq_rn")


        total_rows = df.count()
        if total_rows == 0:
            logger.warning(f"[DQ-EMPTY] {table_name}: empty dataframe (run_id={run_id})")
            return df

        checked_df = df

        # 2. Evaluate all rules
        for r in rules:
            checked_df = checked_df.withColumn(
                f"__dq_flag_{r.rule_id}",
                F.expr(f"COALESCE(({r.sql_condition}), false)")
            )

        checked_df.cache()

        # 3. Check HARD rules
        for r in rules:
            if r.severity == "HARD":
                bad_cnt = checked_df.filter(~F.col(f"__dq_flag_{r.rule_id}")).count()
                if bad_cnt > 0:
                    checked_df.unpersist()
                    raise RuntimeError(
                        f"[DQ HARD FAIL] {table_name} | {r.rule_id} ({r.description}): "
                        f"violated in {bad_cnt}/{total_rows} rows."
                    )

        # 4. Process QUARANTINE rules
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
                logger.warning(f"[DQ QUARANTINE] {table_name}: {q_cnt} rows sent to quarantine")
                # Use saveAsTable for Iceberg append to avoid writeTo API inconsistencies
                quarantine_df.write.mode("append").saveAsTable(f"lakehouse.quarantine.{table_name}")
            
            clean_df = checked_df.filter(valid_expr).drop(*flag_cols)
        else:
            clean_df = checked_df.drop(*flag_cols)

        checked_df.unpersist()
        
        clean_cnt = clean_df.count()
        if clean_cnt < total_rows:
            logger.info(f"[DQ PASS] {table_name}: {total_rows - clean_cnt} rows filtered, {clean_cnt} remaining")
            
        return clean_df