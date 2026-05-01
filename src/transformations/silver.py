from __future__ import annotations

from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.ingestion.schema_mapping import canonicalize_source
from src.transformations.cleaning import standardize_supplier_records
from src.utils.config import PROJECT_ROOT
from src.utils.spark import read_table, write_table


def build_silver_supplier_standardized(
    spark: SparkSession,
    mapping_config: dict,
    bronze_root: str | Path = PROJECT_ROOT / "data" / "bronze",
    silver_root: str | Path = PROJECT_ROOT / "data" / "silver",
) -> DataFrame:
    canonical_columns = mapping_config["canonical_columns"]
    mapped_frames = []

    for source_name, source_config in mapping_config["sources"].items():
        bronze_df = read_table(spark, Path(bronze_root) / source_name)
        mapped_frames.append(
            canonicalize_source(
                df=bronze_df,
                source_name=source_name,
                source_config=source_config,
                canonical_columns=canonical_columns,
            )
        )

    if not mapped_frames:
        raise ValueError("No source frames were available to build Silver")

    unified = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), mapped_frames)
    silver_df = standardize_supplier_records(unified, mapping_config.get("category_aliases", {}))
    write_table(silver_df, Path(silver_root) / "supplier_standardized")
    return silver_df
