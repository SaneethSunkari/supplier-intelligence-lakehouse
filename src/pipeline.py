from __future__ import annotations

from pathlib import Path

from src.data_quality.framework import run_quality_checks
from src.deduplication.supplier_deduper import deduplicate_suppliers
from src.ingestion.bronze import ingest_all_sources, new_batch_id
from src.transformations.gold import build_gold_tables
from src.transformations.silver import build_silver_supplier_standardized
from src.utils.config import PROJECT_ROOT, load_dq_rules, load_schema_mapping
from src.utils.spark import get_spark, write_table


def run_pipeline(batch_id: str | None = None) -> dict[str, str]:
    spark = get_spark()
    try:
        active_batch_id = batch_id or new_batch_id()
        mapping_config = load_schema_mapping()
        dq_config = load_dq_rules()

        ingest_all_sources(spark, mapping_config, batch_id=active_batch_id)
        silver_df = build_silver_supplier_standardized(spark, mapping_config)

        dedup_df = deduplicate_suppliers(silver_df)
        write_table(dedup_df, PROJECT_ROOT / "data" / "silver" / "supplier_deduplicated")

        dq_df = run_quality_checks(
            spark=spark,
            table_frames={
                "silver_supplier_standardized": silver_df,
                "silver_supplier_deduplicated": dedup_df,
            },
            dq_config=dq_config,
        )

        gold_tables = build_gold_tables(dedup_df, dq_df)
        for table_name, table_df in gold_tables.items():
            write_table(table_df, Path(PROJECT_ROOT / "data" / "gold" / table_name))

        return {
            "batch_id": active_batch_id,
            "bronze_path": str(PROJECT_ROOT / "data" / "bronze"),
            "silver_path": str(PROJECT_ROOT / "data" / "silver"),
            "gold_path": str(PROJECT_ROOT / "data" / "gold"),
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    result = run_pipeline()
    for key, value in result.items():
        print(f"{key}: {value}")
