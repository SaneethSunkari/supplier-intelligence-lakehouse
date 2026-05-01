from src.transformations.silver import build_silver_supplier_standardized
from src.utils.config import load_schema_mapping
from src.utils.spark import get_spark


spark = get_spark("SourceIQ 02 Silver Cleaning")
mapping_config = load_schema_mapping()

silver_df = build_silver_supplier_standardized(spark, mapping_config)
print(f"Silver supplier_standardized rows: {silver_df.count()}")
silver_df.select(
    "supplier_name",
    "supplier_name_normalized",
    "email_clean",
    "phone_digits",
    "category_standardized",
    "completeness_score",
).show(truncate=False)
