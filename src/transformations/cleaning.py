from __future__ import annotations

from pyspark.sql import DataFrame, functions as F, types as T

from src.utils.normalization import (
    EMAIL_PATTERN,
    TAX_ID_PATTERN,
    match_key_text,
    normalize_supplier_name_text,
)


normalize_supplier_name_udf = F.udf(normalize_supplier_name_text, T.StringType())
match_key_udf = F.udf(match_key_text, T.StringType())


def standardize_supplier_records(df: DataFrame, category_aliases: dict[str, str] | None = None) -> DataFrame:
    aliases = category_aliases or {}
    category_map = F.create_map(
        *[item for pair in aliases.items() for item in (F.lit(pair[0].lower()), F.lit(pair[1]))]
    )

    phone_digits = F.regexp_replace(F.coalesce(F.col("phone"), F.lit("")), r"[^0-9]", "")
    phone_digits = F.when(
        (F.length(phone_digits) == 11) & phone_digits.startswith("1"),
        F.substring(phone_digits, 2, 10),
    ).otherwise(phone_digits)

    quantity_ordered = F.col("quantity").cast("double")
    unit_price_amount = F.col("unit_price").cast("double")
    negotiated_price_amount = F.col("negotiated_price").cast("double")
    annual_spend_amount = F.coalesce(
        F.col("annual_spend").cast("double"),
        quantity_ordered * negotiated_price_amount,
        quantity_ordered * unit_price_amount,
    )

    standardized = (
        df.withColumn("supplier_name_clean", F.initcap(F.trim(F.regexp_replace(F.col("supplier_name"), r"_+", " "))))
        .withColumn("supplier_name_normalized", normalize_supplier_name_udf("supplier_name"))
        .withColumn("supplier_name_match_key", match_key_udf("supplier_name"))
        .withColumn("email_clean", F.lower(F.trim(F.col("email"))))
        .withColumn("email_is_valid", F.col("email_clean").rlike(EMAIL_PATTERN))
        .withColumn("phone_digits", F.when(phone_digits == "", F.lit(None)).otherwise(phone_digits))
        .withColumn("phone_is_valid", F.length(F.col("phone_digits")) == 10)
        .withColumn("address_clean", F.initcap(F.trim(F.regexp_replace(F.col("address"), r"\s+", " "))))
        .withColumn("city_clean", F.initcap(F.trim(F.col("city"))))
        .withColumn("state_clean", F.upper(F.trim(F.col("state"))))
        .withColumn(
            "country_clean",
            F.when(F.upper(F.trim(F.col("country"))).isin("USA", "UNITED STATES", "US"), "US").otherwise(
                F.upper(F.trim(F.col("country")))
            ),
        )
        .withColumn(
            "category_standardized",
            F.coalesce(category_map[F.lower(F.trim(F.col("category")))], F.initcap(F.trim(F.col("category")))),
        )
        .withColumn("naics_code_clean", F.regexp_replace(F.col("naics_code"), r"[^0-9]", ""))
        .withColumn("tax_id_clean", F.upper(F.trim(F.col("tax_id"))))
        .withColumn("tax_id_is_valid", F.col("tax_id_clean").rlike(TAX_ID_PATTERN))
        .withColumn("quantity_ordered", quantity_ordered)
        .withColumn("unit_price_amount", unit_price_amount)
        .withColumn("negotiated_price_amount", negotiated_price_amount)
        .withColumn("annual_spend_amount", annual_spend_amount)
        .withColumn("activity_count", F.col("activity_count").cast("int"))
        .withColumn("last_activity_date", F.to_date("last_activity_date"))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("delivery_date", F.to_date("delivery_date"))
        .withColumn("delivery_days", F.datediff("delivery_date", "order_date"))
        .withColumn("defective_units_count", F.col("defective_units").cast("double"))
        .withColumn("compliance_status_clean", F.initcap(F.trim(F.col("compliance_status"))))
        .withColumn("order_status_clean", F.initcap(F.trim(F.col("order_status"))))
        .withColumn(
            "risk_signal",
            F.coalesce(
                F.lower(F.trim(F.col("risk_signal"))),
                F.when(
                    (F.col("compliance_status_clean") == "No") | (F.col("defective_units_count") >= 100),
                    "high",
                )
                .when(
                    (F.col("defective_units_count") > 0) | (F.col("order_status_clean") != "Delivered"),
                    "medium",
                )
                .when(F.col("po_id").isNotNull(), "low"),
            ),
        )
    )

    supplier_profile_columns = [
        "supplier_name_clean",
        "email_clean",
        "phone_digits",
        "city_clean",
        "state_clean",
        "country_clean",
        "category_standardized",
        "naics_code_clean",
    ]
    supplier_profile_filled_count = sum(
        F.when(F.col(column).isNotNull() & (F.trim(F.col(column).cast("string")) != ""), 1).otherwise(0)
        for column in supplier_profile_columns
    )
    transaction_columns = [
        "supplier_name_clean",
        "category_standardized",
        "order_date",
        "annual_spend_amount",
        "order_status_clean",
    ]
    transaction_filled_count = sum(
        F.when(F.col(column).isNotNull() & (F.trim(F.col(column).cast("string")) != ""), 1).otherwise(0)
        for column in transaction_columns
    )
    return standardized.withColumn(
        "completeness_score",
        F.when(
            F.col("source_system") == "kaggle_procurement_kpi",
            F.round(transaction_filled_count / F.lit(float(len(transaction_columns))), 4),
        ).otherwise(F.round(supplier_profile_filled_count / F.lit(float(len(supplier_profile_columns))), 4)),
    )
