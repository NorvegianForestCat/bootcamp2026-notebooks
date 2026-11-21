import logging
import os
from pyspark.sql import DataFrame, functions as fn
from common.lakehouse_spark import SparkSessionContextManager


logging.basicConfig(
    level=logging.INFO,
    style="{",
    format="{asctime} [{levelname}] {message}"
)

def write_table(
        ctx:SparkSessionContextManager, 
        df:DataFrame, 
        target_table:str):
    ctx.sql("create namespace if not exists lakehouse.raw")
    
    now_ts = fn.current_timestamp()
    
    if "loaded_at" in df.columns:
        src_ts = fn.col("loaded_at")
    elif "created_at" in df.columns:
        src_ts = fn.col("created_at")
    else:
        src_ts = now_ts

    df_raw = df.withColumn("src_modified_stamp", src_ts).withColumn(
        "sys_insert_stamp", now_ts
    )
    if "loaded_at" not in df_raw.columns:
        df_raw = df_raw.withColumn("loaded_at", now_ts)

    if not ctx.catalog.tableExists(target_table):
        df_raw.writeTo(target_table).using("iceberg").createOrReplace()
    else:
        df_raw.writeTo(target_table).append()

def extract_postgres(
        ctx:SparkSessionContextManager, 
        pg_url:str, 
        pg_user:str, 
        pg_password:str) -> None:
    postgre_df = (
        ctx.read.format("jdbc")
        .option("url", pg_url)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .option(
            "dbtable",
            "(select table_name from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE') as t"
        ).load()
    )

    tables = [
        row["table_name"]
        for row in postgre_df.collect()
        if not row["table_name"].startswith("iceberg_")
    ]

    for table in tables:
        target = f"lakehouse.raw.pg__{table}"

        with ctx.lineage(
            f"extract-pg-{table}", 
            [f"postgres.{table}"], 
            target):
            df = (
                ctx.read.format("jdbc")
                .option("url", pg_url)
                .option("user", pg_user)
                .option("password", pg_password)
                .option("dbtable", table)
                .option("driver", "org.postgresql.Driver")
                .load()
            )

            write_table(ctx, df, target)

            logging.info(f"[extract_pg->raw] {table} -> {target}")

def extract_mongo(ctx:SparkSessionContextManager,
                  mongo_uri:str,
                  mongo_db:str,
                  collections:list[str] | None = None) -> None:
    if not collections:
        client = ctx._jvm.com.mongodb.client.MongoClients.create(mongo_uri)
    
        try:
            collections = [
                name
                for name in client.getDatabase(mongo_db).listCollectionNames()
                if not name.startswith("system.")
            ]
        finally:
            client.close()

    for coll in collections:
        target = f"lakehouse.raw.mongo__{coll}"
        with ctx.lineage(
            f"extract-mongo-{coll}",
            inputs=[f"mongo.{mongo_db}.{coll}"],
            output=target):
            df = (
                ctx.read.format("mongodb")
                .option("spark.mongodb.read.connection.uri", mongo_uri)
                .option("spark.mongodb.read.database", mongo_db)
                .option("spark.mongodb.read.collection", coll)
                .load())

            write_table(ctx, df, target)
            
            logging.info(f"[extract_mongo->raw] {coll} -> {target}")

def main() -> None:
    pg_user = os.environ["PG_USER"]
    pg_password = os.environ["PG_PASSWORD"]
    pg_port = os.getenv("PG_PORT", 5432)
    pg_db = os.getenv("PG_DB", "dota")
    pg_url = f"jdbc:postgresql://postgres:{pg_port}/{pg_db}"

    mongo_user = os.environ["MONGO_USER"]
    mongo_password = os.environ["MONGO_PASSWORD"]
    mongo_db = os.getenv("MONGO_DB", "dota_events")
    mongo_port = os.getenv("MONGO_PORT", 27017)
    mongo_uri = (
        f"mongodb://{mongo_user}:{mongo_password}@mongo:{mongo_port}/?authSource=admin"
    )

    with SparkSessionContextManager("extract-raw") as ctx:
        extract_postgres(ctx, pg_url, pg_user, pg_password)
        extract_mongo(ctx, mongo_uri, mongo_db)


if __name__ == "__main__":
    main()
