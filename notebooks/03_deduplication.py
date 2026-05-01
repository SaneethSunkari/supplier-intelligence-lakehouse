from src.deduplication.supplier_deduper import deduplicate_suppliers
from src.utils.config import PROJECT_ROOT
from src.utils.spark import get_spark, read_table, write_table


spark = get_spark("SourceIQ 03 Deduplication")
silver_df = read_table(spark, PROJECT_ROOT / "data" / "silver" / "supplier_standardized")

dedup_df = deduplicate_suppliers(silver_df)
write_table(dedup_df, PROJECT_ROOT / "data" / "silver" / "supplier_deduplicated")

dedup_df.select(
    "master_supplier_id",
    "supplier_name",
    "dedupe_key",
    "duplicate_record_count",
    "duplicate_score",
    "is_master_record",
).orderBy("dedupe_key", "record_rank").show(truncate=False)
