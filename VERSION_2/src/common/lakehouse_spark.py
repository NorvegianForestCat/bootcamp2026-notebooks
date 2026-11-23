import os
import uuid
import json
import urllib.request
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from pyspark.sql import SparkSession


class SparkSessionContextManager:
    """
    A context manager for managing a SparkSession.

    Ensures that the SparkSession is properly started and stopped, and
    provides integrated lineage tracking via Marquez for data provenance.

    Usage:
        with SparkSessionContextManager("my-job") as spark:
            df = spark.read.parquet("s3://bucket/data")
            # ... process data
            spark.publish_table("mart", "sales", df, "Monthly sales data")
    """
    def __init__(self, app_name: str) -> None:
        """
        Initialize the SparkSessionContextManager.

        Sets up the application name and reads configuration from environment
        variables for Marquez integration and OpenLineage namespace.

        Args:
            app_name: The application name for the SparkSession.
        """
        self.app_name: str = app_name
        self.spark: SparkSession | None = None
        self.marquez_url: str = os.environ.get("MARQUEZ_URL", "http://marquez:5000")
        self.namespace: str = os.environ.get("OPENLINEAGE_NAMESPACE", "dota-lakehouse")

    def __enter__(self) -> "SparkSessionContextManager":
        builder = SparkSession.Builder().appName(name=self.app_name)

        pg_user = os.environ.get("PG_USER")
        pg_password = os.environ.get("PG_PASSWORD")

        if pg_password and pg_user:
            builder = builder.config("spark.sql.catalog.lakehouse.jdbc.user", pg_user
            ).config("spark.sql.catalog.lakehouse.jdbc.password", pg_password)

        self.spark = builder.getOrCreate()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.spark:
            self.spark.stop()

    def __getattr__(self, name):
        return getattr(self.spark, name)

    @contextmanager
    def lineage(
        self,
        job_name: str,
        inputs: list[str],
        output: str,
        description: str = "",
        dataset_descriptions: dict | None = None
    ):
        """
        Context manager for emitting lineage events to Marquez.

        Tracks the start, completion, or failure of a Spark job with input/output
        datasets for data provenance and auditability.

        Args:
            job_name: The name of the Spark job (used as the Marquez job name).
            inputs: List of input dataset names participating in the job.
            output: The output dataset name produced by the job.
            description: Optional description of the job's purpose or function.
            dataset_descriptions: Optional dict mapping dataset names to descriptions.

        Yields:
            Control is yielded back to the caller after emitting the START event.
            On completion, emits COMPLETE; on exception, emits FAIL.

        Example:
            >>> with spark.lineage("process_sales", ["raw_sales"], "mart_sales",
            ...                   "Monthly sales aggregation") as _:
            ...     result = spark.sql("SELECT * FROM raw_sales")
            ...     result.write.saveAsTable("mart_sales")
        """
        run_uuid = str(uuid.uuid4())
        self._emit_lineage("START", run_uuid, job_name, inputs, output)

        try:
            yield
            self._emit_lineage("COMPLETE", run_uuid, job_name, inputs, output)
        except Exception as e:
            self._emit_lineage("FAIL", run_uuid, job_name, inputs, output)
            raise e

    def _emit_lineage(
            self,
            event_type: str,
            run_id: str,
            job_name: str,
            inputs: list[str],
            output: str
    ) -> None:
        """
        Emits a lineage event to the Marquez API.

        Sends an event payload describing the lifecycle of a dataset pipeline
        run (START/COMPLETE/FAIL) to the Marquez tracking server.

        Args:
            event_type: The type of event ("START", "COMPLETE", or "FAIL").
            run_id: A unique identifier for this execution run.
            job_name: The name of the job associated with the event.
            inputs: List of input dataset names involved in the run.
            output: The output dataset name produced by the run.

        Returns:
            None. Logs errors if the Marquez API request fails.
        """
        payload = json.dumps({
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {"runId": run_id},
            "job": {"namespace": self.namespace, "name": job_name},
            "inputs": [
                {"namespace": self.namespace, "name": name} for name in inputs
            ],
            "outputs": [{"namespace": self.namespace, "name": output}],
            "producer": "urn:dota-lakehouse:spark-etl"
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{self.marquez_url}/api/v1/lineage",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        try:
            with urllib.request.urlopen(req, timeout=5):
                pass
        except Exception as e:
            logging.error(e)
