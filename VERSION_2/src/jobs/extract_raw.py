import logging
import os
from pyspark.sql import functions as fn
from common.lakehouse_spark import SparkSessionContextManager


logging.basicConfig(
    level=logging.INFO,
    style="{",
    format="{asctime} [{levelname}] {message}"
)

def extract_postgres(
        ctx: SparkSessionContextManager, 
        pg_url:str, 
        pg_user:str, 
        pg_password:str):
    pass
