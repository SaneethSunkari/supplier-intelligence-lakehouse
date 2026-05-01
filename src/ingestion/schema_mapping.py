from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


METADATA_COLUMNS = [
    "source_system",
    "ingestion_timestamp",
    "batch_id",
    "raw_file_name",
    "source_priority",
]


def canonicalize_source(
    df: DataFrame,
    source_name: str,
    source_config: dict,
    canonical_columns: list[str],
) -> DataFrame:
    """Map source-specific columns to a standard supplier schema."""
    mapping = source_config.get("column_mapping", {})
    selected = []

    for canonical_col in canonical_columns:
        source_col = mapping.get(canonical_col)
        if source_col and source_col in df.columns:
            selected.append(F.col(source_col).cast("string").alias(canonical_col))
        else:
            selected.append(F.lit(None).cast("string").alias(canonical_col))

    for metadata_col in METADATA_COLUMNS:
        if metadata_col in df.columns:
            selected.append(F.col(metadata_col))
        elif metadata_col == "source_system":
            selected.append(F.lit(source_name).alias(metadata_col))
        elif metadata_col == "source_priority":
            selected.append(F.lit(int(source_config.get("source_priority", 99))).alias(metadata_col))

    return df.select(*selected)
