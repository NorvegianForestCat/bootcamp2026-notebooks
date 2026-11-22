import logging
import os
from pyspark.sql import DataFrame, functions as fn
from common.lakehouse_spark import SparkSessionContextManager

logging.basicConfig(
    level=logging.INFO,
    style="{",
    format="{asctime} [{levelname}] {message}"
)


def get_watermark(ctx: SparkSessionContextManager, target_table: str) -> str | None:
    """
    Retrieves the maximum timestamp (loaded_at or sys_insert_stamp) from the target table.

    Used for incremental load watermark detection to identify new/updated records.

    Args:
        ctx: SparkSessionContextManager providing the Spark session.
        target_table: The full Iceberg table identifier (e.g. 'lakehouse.raw.pg__matches').

    Returns:
        String representation of the maximum timestamp, or None if the table
        doesn't exist or has no data.
    """
    try:
        if ctx.catalog.tableExists(target_table):
            res = ctx.sql(
                f"SELECT MAX(COALESCE(loaded_at, sys_insert_stamp)) as max_ts FROM {target_table}"
            ).collect()
            if res and res[0]["max_ts"]:
                return str(res[0]["max_ts"])
    except Exception as e:
        logging.warning(f"Failed to get watermark for {target_table}: {e}")
    return None


def write_table(
    ctx: SparkSessionContextManager,
    df: DataFrame,
    target_table: str,
    pk_col: str | None = None
) -> None:
    """
    Writes a DataFrame to the raw lakehouse layer as an Iceberg table with
    incremental load support.

    Algorithm:
    1. If table does not exist -> creates it (createOrReplace).
    2. If table exists -> retrieves watermark (MAX loaded_at).
    3. Filters incoming data: keeps only records newer than watermark.
    4. If new data and pk_col specified -> performs MERGE INTO (upsert).
    5. If pk_col not specified -> performs safe append (new records only).

    The function handles timestamp column selection preferring 'loaded_at' over
    'created_at', and falls back to the current timestamp if neither is present.
    System metadata columns (src_modified_stamp, sys_insert_stamp, loaded_at)
    are automatically added.

    Args:
        ctx: SparkSessionContextManager providing the Spark session and
             lineage tracking capabilities.
        df: The input DataFrame to persist to the raw layer.
        target_table: The full Iceberg table identifier (e.g.
                      'lakehouse.raw.pg__my_table').
        pk_col: Optional primary column name for MERGE/Upsert operation (e.g.
                'match_id' or '_id'). If None, uses safe append mode.

    Returns:
        None. Side effect: data is written to the lakehouse.

    Notes:
        - Adds `src_modified_stamp` with the source timestamp (or current
          timestamp if no source column exists).
        - Adds `sys_insert_stamp` with the ingestion timestamp.
        - Adds `loaded_at` with the ingestion timestamp if not already present.
        - Uses Iceberg merge semantics when pk_col is provided; otherwise
          appends new records filtered by watermark.
    """
    ctx.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.raw")

    now_ts = fn.current_timestamp()

    # Determine the source timestamp for src_modified_stamp
    if "loaded_at" in df.columns:
        src_ts = fn.col("loaded_at")
    elif "created_at" in df.columns:
        src_ts = fn.col("created_at")
    else:
        src_ts = now_ts

    df_raw = df.withColumn("src_modified_stamp", src_ts).withColumn("sys_insert_stamp", now_ts)
    if "loaded_at" not in df_raw.columns:
        df_raw = df_raw.withColumn("loaded_at", now_ts)

    # If table does not exist, create it
    if not ctx.catalog.tableExists(target_table):
        df_raw.writeTo(target_table).using("iceberg").createOrReplace()
        logging.info(f"[extract] New table created: {target_table}")
        return

    # Incremental filter by watermark
    watermark = get_watermark(ctx, target_table)
    if watermark:
        df_raw = df_raw.filter(fn.col("src_modified_stamp") > fn.lit(watermark))

    # Check for new records
    if df_raw.rdd.isEmpty():
        logging.info(f"[extract] No new data for {target_table} (watermark: {watermark})")
        return

    # Upsert via MERGE INTO if primary key is known
    if pk_col and pk_col in df_raw.columns:
        df_raw.createOrReplaceTempView("new_data")
        ctx.sql(f"""
            MERGE INTO {target_table} AS target
            USING new_data AS source
            ON target.{pk_col} = source.{pk_col}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        logging.info(f"[extract] MERGE INTO executed in {target_table} on key {pk_col}")
    else:
        # Fallback: safe append for new records (thanks to watermark filtering)
        df_raw.writeTo(target_table).append()
        logging.info(f"[extract] APPEND executed in {target_table} (no explicit PK)")


def extract_postgres(
    ctx: SparkSessionContextManager,
    pg_url: str,
    pg_user: str,
    pg_password: str
) -> None:
    """
    Extracts tables from PostgreSQL and writes them to the raw lakehouse layer.

    Reads table names from the information_schema, filters out system tables
    (those starting with 'iceberg_'), and for each user table extracts the
    data via JDBC and persists it to `lakehouse.raw.pg__{table}`.

    Supports incremental loading with MERGE INTO upserts using a primary key
    (default 'match_id' for matches and match_players tables).

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
            "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE') AS t"
        ).load()
    )

    tables = [
        row["table_name"]
        for row in postgre_df.collect()
        if not row["table_name"].startswith("iceberg_")
    ]

    for table in tables:
        target = f"lakehouse.raw.pg__{table}"
        
        # Determine the primary key for MERGE (for most tables it's match_id)
        pk = "match_id" if table in ["matches", "match_players"] else None

        with ctx.lineage(
            f"extract-pg-{table}",
            [f"postgres.{table}"],
            target
        ):
            df = (
                ctx.read.format("jdbc")
                .option("url", pg_url)
                .option("user", pg_user)
                .option("password", pg_password)
                .option("dbtable", table)
                .option("driver", "org.postgresql.Driver")
                .load()
            )

            write_table(ctx, df, target, pk_col=pk)
            logging.info(f"[extract_pg->raw] {table} -> {target}")


def extract_mongo(
    ctx: SparkSessionContextManager,
    mongo_uri: str,
    mongo_db: str,
    collections: list[str] | None = None
) -> None:
    """
    Extracts collections from MongoDB and writes them to the raw lakehouse layer.

    If no collection list is provided, discovers collections from the MongoDB
    database (filtering out system collections). For each collection, reads the
    data and persists it to `lakehouse.raw.mongo__{collection_name}`.

    Supports incremental loading with MERGE INTO upserts using the '_id' field
    as the primary key.

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
            output=target
        ):
            df = (
                ctx.read.format("mongodb")
                .option("spark.mongodb.read.connection.uri", mongo_uri)
                .option("spark.mongodb.read.database", mongo_db)
                .option("spark.mongodb.read.collection", coll)
                .load()
            )

            # For MongoDB, the primary key is always '_id'
            write_table(ctx, df, target, pk_col="_id")
            logging.info(f"[extract_mongo->raw] {coll} -> {target}")


def main() -> None:
    """
    Main entry point for the extract-raw task.

    Orchestrates incremental data extraction from PostgreSQL and MongoDB
    into the raw layer. This is the first step in the ETL pipeline, loading
    raw source data before any transformation or aggregation steps.

    The function:
    1. Reads environment variables for database connection configuration
    2. Creates a SparkSessionContextManager for session management and lineage
    3. Extracts all PostgreSQL tables from the 'public' schema
    4. Extracts MongoDB collections from the configured database
    5. Writes all data to the lakehouse.raw layer as Iceberg tables
       with incremental load support and primary key upserts

    Environment Variables Required:
        PG_USER: PostgreSQL username
        PG_PASSWORD: PostgreSQL password
        PG_PORT: PostgreSQL port (default: 5432)
        PG_DB: PostgreSQL database name (default: 'dota')
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
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB", "dota")
    pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    mongo_host = os.getenv("MONGO_HOST", "mongo")
    mongo_user = os.environ["MONGO_USER"]
    mongo_password = os.environ["MONGO_PASSWORD"]
    mongo_db = os.getenv("MONGO_DB", "dota_events")
    mongo_port = os.getenv("MONGO_PORT", "27017")
    mongo_uri = (
        f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin"
    )

    with SparkSessionContextManager("extract-raw") as ctx:
        extract_postgres(ctx, pg_url, pg_user, pg_password)
        extract_mongo(ctx, mongo_uri, mongo_db)


if __name__ == "__main__":
    main()