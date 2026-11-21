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
        ctx: SparkSessionContextManager,
        df: DataFrame,
        target_table: str) -> None:
    """
    Write a DataFrame to the lakehouse raw layer as an Iceberg table.

    Creates the namespace if it doesn't exist, adds system metadata columns
    (src_modified_stamp, sys_insert_stamp, loaded_at), and writes to or
    appends into the target Iceberg table.

    The function handles timestamp column selection preferring 'loaded_at'
    over 'created_at', and falls back to the current timestamp if neither
    is present.

    Args:
        ctx: SparkSessionContextManager providing the Spark session and
             lineage tracking capabilities.
        df: The input DataFrame to persist to the raw layer.
        target_table: The full Iceberg table identifier (e.g.
                      'lakehouse.raw.pg__my_table').

    Returns:
        None. Side effect: data is written to the lakehouse.

    Notes:
        - Adds `src_modified_stamp` with the source timestamp (or current
          timestamp if no source column exists).
        - Adds `sys_insert_stamp` with the ingestion timestamp.
        - Adds `loaded_at` with the ingestion timestamp if not already present.
        - Uses Iceberg MERGE semantics: creates the table if it doesn't
          exist, otherwise appends new records.
    """
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
        ctx: SparkSessionContextManager,
        pg_url: str,
        pg_user: str,
        pg_password: str) -> None:
    """
    Extract all PostgreSQL tables and write them to the lakehouse raw layer.

    Reads table names from the information_schema, filters out system tables
    (those starting with 'iceberg_'), and for each user table extracts the
    data via JDBC and persists it to `lakehouse.raw.pg__{table}`.

    Emits lineage events via the SparkSessionContextManager for each table
    extracted, enabling data provenance tracking in Marquez.

    Args:
        ctx: SparkSessionContextManager for Spark session and lineage tracking.
        pg_url: JDBC URL for the PostgreSQL connection (e.g.
                'jdbc:postgresql://postgres:5432/dota').
        pg_user: PostgreSQL username for authentication.
        pg_password: PostgreSQL password for authentication.

    Returns:
        None. Side effects: tables are extracted from PostgreSQL and written
        to the lakehouse raw layer; lineage events are emitted.

    Example:
        >>> extract_postgres(ctx,
        ...                  'jdbc:postgresql://postgres:5432/dota',
        ...                  'dota_user',
        ...                  'dota_password')
    """
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

def extract_mongo(ctx: SparkSessionContextManager,
                  mongo_uri: str,
                  mongo_db: str,
                  collections: list[str] | None = None) -> None:
    """
    Extract MongoDB collections and write them to the lakehouse raw layer.

    If no collection list is provided, discovers collections from the MongoDB
    database (filtering out system collections). For each collection, reads the
    data and persists it to `lakehouse.raw.mongo__{collection_name}`.

    Emits lineage events via the SparkSessionContextManager for each collection
    extracted, enabling data provenance tracking in Marquez.

    Args:
        ctx: SparkSessionContextManager for Spark session and lineage tracking.
        mongo_uri: MongoDB connection URI (e.g.
                   'mongodb://user:password@mongo:27017/?authSource=admin').
        mongo_db: The MongoDB database name to extract from (e.g. 'dota_events').
        collections: Optional list of collection names to extract. If None,
                     all non-system collections in the database will be discovered.

    Returns:
        None. Side effects: collections are extracted from MongoDB and written
        to the lakehouse raw layer; lineage events are emitted.

    Example:
        >>> extract_mongo(ctx,
        ...                'mongodb://user:pass@mongo:27017/?authSource=admin',
        ...                'dota_events',
        ...                ['cs_events', 'gold_events'])
    """
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
    """
    Main entry point for the extract-raw job.

    Orchestrates the extraction of data from PostgreSQL and MongoDB source
    systems into the lakehouse raw layer. This is the first step in the
    ETL pipeline, loading raw source data before any transformation or
    aggregation steps.

    The function:
    1. Reads environment variables for database connection configuration
    2. Creates a SparkSessionContextManager for session management and lineage
    3. Extracts all PostgreSQL tables from the 'public' schema
    4. Extracts MongoDB collections from the configured database
    5. Writes all data to the lakehouse.raw layer as Iceberg tables

    Environment Variables Required:
        PG_HOST: PostgreSQL host (default: 'postgres')
        PG_USER: PostgreSQL username
        PG_PASSWORD: PostgreSQL password
        PG_PORT: PostgreSQL port (default: 5432)
        PG_DB: PostgreSQL database name (default: 'dota')
        MONGO_HOST: MongoDB host (default: 'mongo')
        MONGO_USER: MongoDB username
        MONGO_PASSWORD: MongoDB password
        MONGO_DB: MongoDB database name (default: 'dota_events')
        MONGO_PORT: MongoDB port (default: 27017)

    Returns:
        None. Side effects: data is extracted from source systems and written
        to the lakehouse raw layer.

    Example:
        >>> python -m src.jobs.extract_raw
        # Extracts PostgreSQL and MongoDB data to lakehouse.raw
    """
    pg_host = os.getenv("PG_HOST", "postgres")
    pg_user = os.environ["PG_USER"]
    pg_password = os.environ["PG_PASSWORD"]
    pg_port = os.getenv("PG_PORT", 5432)
    pg_db = os.getenv("PG_DB", "dota")
    pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    mongo_host = os.getenv("MONGO_HOST", "mongo")
    mongo_user = os.environ["MONGO_USER"]
    mongo_password = os.environ["MONGO_PASSWORD"]
    mongo_db = os.getenv("MONGO_DB", "dota_events")
    mongo_port = os.getenv("MONGO_PORT", 27017)
    mongo_uri = (
        f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin"
    )

    with SparkSessionContextManager("extract-raw") as ctx:
        extract_postgres(ctx, pg_url, pg_user, pg_password)
        extract_mongo(ctx, mongo_uri, mongo_db)


if __name__ == "__main__":
    main()