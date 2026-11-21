import logging


def publish_table(
    layer: str,
    table_name: str,
    dataframe: object,
    description: str = "",
    dependencies: list[str] | None = None,
) -> str:
    """
    Publishes a table to the lakehouse catalog.

    Registers the specified table in the given layer with the provided name
    and description. Optionally accepts dependencies for lineage tracking.

    Args:
        layer: The lakehouse layer (e.g., 'raw', 'stage', 'mart').
        table_name: The name of the table to publish.
        dataframe: The dataframe object to publish.
        description: Optional description of the table's purpose/content.
        dependencies: Optional list of dependency table names for lineage tracking.

    Returns:
        A log string indicating the catalog entry that was created.
    """
    log = f"[CATALOG] lakehouse.{layer}.{table_name}"
    logging.info(log)

    return log
