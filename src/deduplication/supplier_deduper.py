from __future__ import annotations

from difflib import SequenceMatcher
from typing import Optional

from pyspark.sql import DataFrame, Window, functions as F, types as T


def name_similarity(left: Optional[str], right: Optional[str]) -> float:
    if not left or not right:
        return 0.0
    try:
        from rapidfuzz import fuzz

        return float(fuzz.token_set_ratio(left, right))
    except Exception:
        return round(SequenceMatcher(None, left, right).ratio() * 100, 2)


name_similarity_udf = F.udf(name_similarity, T.DoubleType())


def deduplicate_suppliers(df: DataFrame) -> DataFrame:
    exact_phone_key = F.when(F.col("phone_is_valid"), F.concat(F.lit("phone:"), F.col("phone_digits")))
    exact_email_key = F.when(F.col("email_is_valid"), F.concat(F.lit("email:"), F.col("email_clean")))
    fuzzy_name_key = F.when(
        F.col("supplier_name_match_key").isNotNull() & F.col("city_clean").isNotNull() & F.col("state_clean").isNotNull(),
        F.concat_ws(":", F.lit("name_city_state"), F.col("supplier_name_match_key"), F.col("city_clean"), F.col("state_clean")),
    )
    exact_name_key = F.when(
        F.col("supplier_name_match_key").isNotNull(),
        F.concat_ws(":", F.lit("name"), F.col("supplier_name_match_key")),
    )

    keyed = df.withColumn(
        "dedupe_key",
        F.coalesce(exact_phone_key, exact_email_key, fuzzy_name_key, exact_name_key, F.sha2(F.concat_ws("||", *df.columns), 256)),
    ).withColumn("master_supplier_id", F.sha2(F.col("dedupe_key"), 256))

    group_window = Window.partitionBy("dedupe_key")
    rank_window = Window.partitionBy("dedupe_key").orderBy(
        F.desc("completeness_score"),
        F.asc("source_priority"),
        F.desc("ingestion_timestamp"),
    )

    ranked = (
        keyed.withColumn("duplicate_record_count", F.count("*").over(group_window))
        .withColumn("record_rank", F.row_number().over(rank_window))
        .withColumn("is_master_record", F.col("record_rank") == 1)
    )

    master_window = rank_window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    scored_base = ranked.withColumn(
        "master_supplier_name_normalized",
        F.first("supplier_name_normalized", ignorenulls=True).over(master_window),
    )

    max_name_length = F.greatest(
        F.length(F.coalesce(F.col("supplier_name_normalized"), F.lit(""))),
        F.length(F.coalesce(F.col("master_supplier_name_normalized"), F.lit(""))),
    )
    fuzzy_score = F.when(max_name_length == 0, F.lit(0.0)).otherwise(
        F.round(
            (F.lit(1.0) - F.levenshtein("supplier_name_normalized", "master_supplier_name_normalized") / max_name_length)
            * F.lit(100.0),
            2,
        )
    )

    scored = scored_base.withColumn(
        "duplicate_score",
        F.when(F.col("duplicate_record_count") <= 1, F.lit(0.0))
        .when(F.col("dedupe_key").startswith("phone:"), F.lit(100.0))
        .when(F.col("dedupe_key").startswith("email:"), F.lit(100.0))
        .otherwise(fuzzy_score),
    )

    return scored.drop("master_supplier_name_normalized")
