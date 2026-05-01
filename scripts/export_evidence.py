from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils.config import PROJECT_ROOT
from src.utils.spark import get_spark, read_table


EVIDENCE_DIR = PROJECT_ROOT / "evidence"
BRONZE_SOURCES = [
    "conference_csv",
    "government_vendor",
    "erp_export",
    "supplier_activity_api",
    "kaggle_procurement_kpi",
]
SILVER_TABLES = ["supplier_standardized", "supplier_deduplicated"]
GOLD_TABLES = [
    "gold_supplier_master",
    "gold_supplier_quality_metrics",
    "gold_supplier_spend_summary",
    "gold_supplier_category_analytics",
    "gold_ml_supplier_features",
]


def md_table(pdf: pd.DataFrame) -> str:
    pdf = pdf.fillna("")
    headers = list(pdf.columns)
    rows = pdf.astype(str).values.tolist()
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        safe = [cell.replace("\n", " ").replace("|", "\\|") for cell in row]
        lines.append("| " + " | ".join(safe) + " |")
    return "\n".join(lines)


def export_table_counts(spark) -> pd.DataFrame:
    counts = []
    for source in BRONZE_SOURCES:
        df = read_table(spark, PROJECT_ROOT / "data" / "bronze" / source)
        counts.append({"layer": "bronze", "table": source, "rows": df.count(), "columns": len(df.columns)})
        df.limit(1000).toPandas().to_csv(EVIDENCE_DIR / f"bronze_{source}.csv", index=False)

    for table in SILVER_TABLES:
        df = read_table(spark, PROJECT_ROOT / "data" / "silver" / table)
        counts.append({"layer": "silver", "table": table, "rows": df.count(), "columns": len(df.columns)})
        if table == "supplier_standardized":
            keep = [
                "source_system",
                "supplier_name",
                "supplier_name_clean",
                "supplier_name_normalized",
                "email_clean",
                "email_is_valid",
                "phone_digits",
                "phone_is_valid",
                "category_standardized",
                "order_date",
                "delivery_date",
                "order_status_clean",
                "quantity_ordered",
                "negotiated_price_amount",
                "annual_spend_amount",
                "defective_units_count",
                "compliance_status_clean",
                "risk_signal",
                "completeness_score",
            ]
        else:
            keep = [
                "master_supplier_id",
                "source_system",
                "supplier_name_clean",
                "supplier_name_normalized",
                "dedupe_key",
                "duplicate_record_count",
                "duplicate_score",
                "record_rank",
                "is_master_record",
                "annual_spend_amount",
                "risk_signal",
                "completeness_score",
            ]
        df.select(*[column for column in keep if column in df.columns]).orderBy("supplier_name_clean").limit(1000).toPandas().to_csv(
            EVIDENCE_DIR / f"silver_{table}.csv", index=False
        )

    for table in GOLD_TABLES:
        df = read_table(spark, PROJECT_ROOT / "data" / "gold" / table)
        counts.append({"layer": "gold", "table": table, "rows": df.count(), "columns": len(df.columns)})
        df.toPandas().to_csv(EVIDENCE_DIR / f"{table}.csv", index=False)

    counts_pdf = pd.DataFrame(counts)
    counts_pdf.to_csv(EVIDENCE_DIR / "table_counts.csv", index=False)
    return counts_pdf


def export_walkthrough(spark, counts_pdf: pd.DataFrame) -> None:
    standardized = read_table(spark, PROJECT_ROOT / "data" / "silver" / "supplier_standardized")
    dedup = read_table(spark, PROJECT_ROOT / "data" / "silver" / "supplier_deduplicated")
    master = read_table(spark, PROJECT_ROOT / "data" / "gold" / "gold_supplier_master")
    dq = read_table(spark, PROJECT_ROOT / "data" / "gold" / "gold_supplier_quality_metrics")
    spend = read_table(spark, PROJECT_ROOT / "data" / "gold" / "gold_supplier_spend_summary")
    ml_features = read_table(spark, PROJECT_ROOT / "data" / "gold" / "gold_ml_supplier_features")

    with (EVIDENCE_DIR / "run_walkthrough.md").open("w", encoding="utf-8") as handle:
        handle.write("# SourceIQ End-to-End Run Evidence\n\n")
        handle.write("This run includes the Kaggle Procurement KPI Analysis Dataset as `kaggle_procurement_kpi`.\n\n")
        handle.write("## Layer Counts\n\n")
        handle.write(md_table(counts_pdf))

        handle.write("\n\n## Kaggle Procurement Sample After Standardization\n\n")
        kaggle_view = (
            standardized.where("source_system = 'kaggle_procurement_kpi'")
            .select(
                "supplier_name_clean",
                "category_standardized",
                "order_date",
                "delivery_date",
                "order_status_clean",
                "quantity_ordered",
                "negotiated_price_amount",
                "annual_spend_amount",
                "defective_units_count",
                "compliance_status_clean",
                "risk_signal",
                "completeness_score",
            )
            .orderBy("order_date")
            .limit(20)
            .toPandas()
        )
        handle.write(md_table(kaggle_view))

        handle.write("\n\n## Duplicate Groups Sample\n\n")
        duplicate_view = (
            dedup.select(
                "supplier_name_clean",
                "source_system",
                "dedupe_key",
                "duplicate_record_count",
                "duplicate_score",
                "record_rank",
                "is_master_record",
                "annual_spend_amount",
                "risk_signal",
            )
            .where("duplicate_record_count > 1")
            .orderBy("dedupe_key", "record_rank")
            .limit(40)
            .toPandas()
        )
        handle.write(md_table(duplicate_view))

        handle.write("\n\n## Master Suppliers\n\n")
        master_view = (
            master.select(
                "supplier_name",
                "category_standardized",
                "state_clean",
                "source_record_count",
                "source_system_count",
                "duplicate_record_count",
                "avg_completeness_score",
                "activity_count",
                "risk_signal",
            )
            .orderBy("supplier_name")
            .toPandas()
        )
        handle.write(md_table(master_view))

        handle.write("\n\n## Spend Summary\n\n")
        spend_view = (
            master.select("master_supplier_id", "supplier_name")
            .join(spend, "master_supplier_id", "left")
            .select("supplier_name", "total_annual_spend", "primary_currency", "currency_count")
            .orderBy("total_annual_spend", ascending=False)
            .toPandas()
        )
        handle.write(md_table(spend_view))

        handle.write("\n\n## ML Supplier Features\n\n")
        ml_view = (
            master.select("master_supplier_id", "supplier_name")
            .join(ml_features, "master_supplier_id", "left")
            .select(
                "supplier_name",
                "has_valid_email",
                "has_valid_phone",
                "avg_completeness_score",
                "duplicate_record_count",
                "source_system_count",
                "total_annual_spend",
                "activity_count",
                "has_high_risk_signal",
            )
            .orderBy("supplier_name")
            .toPandas()
        )
        handle.write(md_table(ml_view))

        handle.write("\n\n## Data Quality Results\n\n")
        dq_view = (
            dq.select("rule_name", "records_checked", "records_failed", "failure_percentage", "quality_status")
            .orderBy("rule_name")
            .toPandas()
        )
        handle.write(md_table(dq_view))
        handle.write("\n")


def main() -> None:
    EVIDENCE_DIR.mkdir(exist_ok=True)
    spark = get_spark("SourceIQ evidence export")
    try:
        spark.sparkContext.setLogLevel("ERROR")
        counts_pdf = export_table_counts(spark)
        export_walkthrough(spark, counts_pdf)
    finally:
        spark.stop()
    print(f"Wrote evidence files to {EVIDENCE_DIR}")


if __name__ == "__main__":
    main()
