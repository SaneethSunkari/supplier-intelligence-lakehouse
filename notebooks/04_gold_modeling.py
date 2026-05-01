from src.data_quality.framework import run_quality_checks
from src.transformations.gold import build_gold_tables
from src.utils.config import PROJECT_ROOT, load_dq_rules
from src.utils.spark import get_spark, read_table, write_table


spark = get_spark("SourceIQ 04 Gold Modeling")
dq_config = load_dq_rules()
dedup_df = read_table(spark, PROJECT_ROOT / "data" / "silver" / "supplier_deduplicated")
standardized_df = read_table(spark, PROJECT_ROOT / "data" / "silver" / "supplier_standardized")

dq_df = run_quality_checks(
    spark=spark,
    table_frames={
        "silver_supplier_standardized": standardized_df,
        "silver_supplier_deduplicated": dedup_df,
    },
    dq_config=dq_config,
)

gold_tables = build_gold_tables(dedup_df, dq_df)
for table_name, table_df in gold_tables.items():
    write_table(table_df, PROJECT_ROOT / "data" / "gold" / table_name)
    print(f"{table_name}: {table_df.count()} records")
