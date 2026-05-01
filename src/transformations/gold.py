from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_supplier_master(dedup_df: DataFrame) -> DataFrame:
    aggregates = dedup_df.groupBy("master_supplier_id").agg(
        F.count("*").alias("source_record_count"),
        F.countDistinct("source_system").alias("source_system_count"),
        F.max("duplicate_record_count").alias("duplicate_record_count"),
        F.avg("completeness_score").alias("avg_completeness_score"),
        F.max("activity_count").alias("activity_count"),
        F.max("last_activity_date").alias("last_activity_date"),
    )

    masters = dedup_df.filter(F.col("is_master_record")).select(
        "master_supplier_id",
        F.col("supplier_name_clean").alias("supplier_name"),
        "supplier_name_normalized",
        "email_clean",
        "phone_digits",
        "address_clean",
        "city_clean",
        "state_clean",
        "country_clean",
        "category_standardized",
        "naics_code_clean",
        "tax_id_clean",
        "small_business_flag",
        "risk_signal",
        "source_system",
        "batch_id",
    )

    return masters.join(aggregates, on="master_supplier_id", how="left")


def build_quality_metrics(dq_df: DataFrame) -> DataFrame:
    return dq_df.withColumn(
        "quality_status",
        F.when(F.col("failure_percentage") == 0, "pass")
        .when(F.col("failure_percentage") <= 0.05, "watch")
        .otherwise("fail"),
    )


def build_spend_summary(dedup_df: DataFrame) -> DataFrame:
    return dedup_df.groupBy("master_supplier_id").agg(
        F.sum(F.coalesce(F.col("annual_spend_amount"), F.lit(0.0))).alias("total_annual_spend"),
        F.countDistinct("currency").alias("currency_count"),
        F.max("currency").alias("primary_currency"),
    )


def build_category_analytics(dedup_df: DataFrame) -> DataFrame:
    return dedup_df.groupBy("category_standardized", "state_clean").agg(
        F.countDistinct("master_supplier_id").alias("supplier_count"),
        F.sum(F.coalesce(F.col("annual_spend_amount"), F.lit(0.0))).alias("total_annual_spend"),
        F.avg("completeness_score").alias("avg_completeness_score"),
        F.max("duplicate_record_count").alias("max_duplicate_record_count"),
    )


def build_ml_supplier_features(dedup_df: DataFrame) -> DataFrame:
    return dedup_df.groupBy("master_supplier_id").agg(
        F.max(F.col("email_is_valid").cast("int")).alias("has_valid_email"),
        F.max(F.col("phone_is_valid").cast("int")).alias("has_valid_phone"),
        F.avg("completeness_score").alias("avg_completeness_score"),
        F.max("duplicate_record_count").alias("duplicate_record_count"),
        F.countDistinct("source_system").alias("source_system_count"),
        F.sum(F.coalesce(F.col("annual_spend_amount"), F.lit(0.0))).alias("total_annual_spend"),
        F.max(F.coalesce(F.col("activity_count"), F.lit(0))).alias("activity_count"),
        F.max(F.when(F.col("risk_signal") == "high", 1).otherwise(0)).alias("has_high_risk_signal"),
    )


def build_gold_tables(dedup_df: DataFrame, dq_df: DataFrame) -> dict[str, DataFrame]:
    return {
        "gold_supplier_master": build_supplier_master(dedup_df),
        "gold_supplier_quality_metrics": build_quality_metrics(dq_df),
        "gold_supplier_spend_summary": build_spend_summary(dedup_df),
        "gold_supplier_category_analytics": build_category_analytics(dedup_df),
        "gold_ml_supplier_features": build_ml_supplier_features(dedup_df),
    }
