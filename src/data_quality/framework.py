from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from src.utils.normalization import EMAIL_PATTERN


DQ_SCHEMA = T.StructType(
    [
        T.StructField("dq_run_id", T.StringType(), False),
        T.StructField("table_name", T.StringType(), False),
        T.StructField("rule_name", T.StringType(), False),
        T.StructField("records_checked", T.LongType(), False),
        T.StructField("records_failed", T.LongType(), False),
        T.StructField("failure_percentage", T.DoubleType(), False),
        T.StructField("run_timestamp", T.TimestampType(), False),
        T.StructField("severity", T.StringType(), True),
    ]
)


def new_dq_run_id() -> str:
    return f"dq-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"


def _failure_condition(rule: dict) -> F.Column:
    column = F.col(rule["column"])
    allow_null = bool(rule.get("allow_null", False))
    rule_type = rule["rule_type"]

    if rule_type == "not_null":
        return column.isNull() | (F.trim(column.cast("string")) == "")

    if rule_type == "email":
        invalid = ~column.rlike(EMAIL_PATTERN)
    elif rule_type == "phone":
        invalid = F.length(column.cast("string")) != 10
    elif rule_type == "regex":
        invalid = ~column.rlike(rule["pattern"])
    elif rule_type == "valid_values":
        invalid = ~column.isin(rule["valid_values"])
    elif rule_type == "min_value":
        invalid = column.cast("double") < float(rule["min_value"])
    elif rule_type == "max_value":
        invalid = column.cast("double") > float(rule["max_value"])
    else:
        raise ValueError(f"Unsupported DQ rule type: {rule_type}")

    if allow_null:
        return column.isNotNull() & (F.trim(column.cast("string")) != "") & invalid
    return column.isNull() | invalid


def evaluate_rule(df: DataFrame, rule: dict) -> dict:
    checked = df.count()
    failed = df.filter(_failure_condition(rule)).count()
    failure_percentage = 0.0 if checked == 0 else round(failed / checked, 4)
    return {
        "table_name": rule["table_name"],
        "rule_name": rule["rule_name"],
        "records_checked": checked,
        "records_failed": failed,
        "failure_percentage": failure_percentage,
        "severity": rule.get("severity"),
    }


def run_quality_checks(
    spark: SparkSession,
    table_frames: dict[str, DataFrame],
    dq_config: dict,
    dq_run_id: str | None = None,
) -> DataFrame:
    active_run_id = dq_run_id or new_dq_run_id()
    run_timestamp = datetime.now(timezone.utc)
    results = []

    for rule in dq_config["rules"]:
        table_name = rule["table_name"]
        if table_name not in table_frames:
            continue
        result = evaluate_rule(table_frames[table_name], rule)
        result["dq_run_id"] = active_run_id
        result["run_timestamp"] = run_timestamp
        results.append(result)

    if not results:
        return spark.createDataFrame([], schema=DQ_SCHEMA)

    ordered = [
        {
            "dq_run_id": row["dq_run_id"],
            "table_name": row["table_name"],
            "rule_name": row["rule_name"],
            "records_checked": row["records_checked"],
            "records_failed": row["records_failed"],
            "failure_percentage": row["failure_percentage"],
            "run_timestamp": row["run_timestamp"],
            "severity": row["severity"],
        }
        for row in results
    ]
    return spark.createDataFrame(ordered, schema=DQ_SCHEMA)
