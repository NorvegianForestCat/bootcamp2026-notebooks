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
    It ensures that the SparkSession is properly started and stopped.
    """
    def __init__(self, app_name: str) -> None:
        self.app_name: str = app_name
        self.spark: SparkSession | None = None
        self.marquez_url:str = os.environ.get("MARQUEZ_URL", "http://marquez:5000")
        self.namespace:str = os.environ.get("OPENLINEAGE_NAMESPACE", "dota-lakehouse")

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
        job_name:str,
        inputs:list[str],
        output:str,
        description:str = "",
        dataset_descriptions:dict | None = None
    ):
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
            event_type:str,
            run_id:str,
            job_name:str,
            inputs:list[str],
            output:str
    ):
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
