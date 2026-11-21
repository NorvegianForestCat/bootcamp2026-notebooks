import logging

def publish_table(
    layer: str,
    table_name: str,
    dataframe: object,
    description: str = "",
    dependencies: list[str] | None = None,
):
    logging.info(
        f"[CATALOG] lakehouse.{layer}.{table_name}"
    )
