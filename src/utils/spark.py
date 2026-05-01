from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def get_spark(app_name: str = "SourceIQ Supplier Intelligence") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
    )

    if lakehouse_format() == "delta":
        try:
            from delta import configure_spark_with_delta_pip

            builder = (
                builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
            )
            return configure_spark_with_delta_pip(builder).getOrCreate()
        except Exception as exc:  # pragma: no cover - depends on local Spark/Delta jars
            raise RuntimeError(
                "LAKEHOUSE_FORMAT=delta requires delta-spark and compatible Spark jars. "
                "Use LAKEHOUSE_FORMAT=parquet for the lightweight local demo."
            ) from exc

    return builder.getOrCreate()


def lakehouse_format() -> str:
    return os.getenv("LAKEHOUSE_FORMAT", "parquet").lower()


def write_table(df: DataFrame, path: str | Path, mode: str = "overwrite") -> None:
    target = str(path)
    fmt = lakehouse_format()
    if fmt == "delta":
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(target)
    else:
        df.write.mode(mode).parquet(target)


def read_table(spark: SparkSession, path: str | Path) -> DataFrame:
    target = str(path)
    if lakehouse_format() == "delta":
        return spark.read.format("delta").load(target)
    return spark.read.parquet(target)
