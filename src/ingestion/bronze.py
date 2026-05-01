from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.utils.config import PROJECT_ROOT, resolve_path
from src.utils.spark import write_table


def new_batch_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{uuid4().hex[:8]}"


def read_source(spark: SparkSession, source_config: dict) -> DataFrame:
    source_path = resolve_path(source_config["path"])
    source_format = source_config["format"].lower()

    if source_format == "csv":
        return spark.read.option("header", True).option("inferSchema", False).csv(str(source_path))
    if source_format == "json":
        return spark.read.option("multiLine", False).json(str(source_path))

    raise ValueError(f"Unsupported source format: {source_format}")


def ingest_source_to_bronze(
    spark: SparkSession,
    source_name: str,
    source_config: dict,
    batch_id: str,
    bronze_root: str | Path = PROJECT_ROOT / "data" / "bronze",
) -> DataFrame:
    bronze_df = (
        read_source(spark, source_config)
        .withColumn("source_system", F.lit(source_name))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("raw_file_name", F.input_file_name())
        .withColumn("source_priority", F.lit(int(source_config.get("source_priority", 99))))
    )

    write_table(bronze_df, Path(bronze_root) / source_name)
    return bronze_df


def ingest_all_sources(
    spark: SparkSession,
    mapping_config: dict,
    batch_id: str | None = None,
    bronze_root: str | Path = PROJECT_ROOT / "data" / "bronze",
) -> dict[str, DataFrame]:
    active_batch_id = batch_id or new_batch_id()
    return {
        source_name: ingest_source_to_bronze(
            spark=spark,
            source_name=source_name,
            source_config=source_config,
            batch_id=active_batch_id,
            bronze_root=bronze_root,
        )
        for source_name, source_config in mapping_config["sources"].items()
    }
