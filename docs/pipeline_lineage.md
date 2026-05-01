# Pipeline Lineage

## Tasks

1. `ingest_raw_sources`
   - Reads CSV and JSON supplier inputs.
   - Writes source-shaped Bronze tables.

2. `validate_bronze`
   - Confirms Bronze outputs exist for all expected sources.

3. `transform_to_silver`
   - Applies `configs/schema_mapping.yaml`.
   - Standardizes supplier fields.
   - Calculates completeness score.

4. `run_deduplication`
   - Groups suppliers by exact phone, exact email, or normalized name plus location.
   - Applies fuzzy name scoring with `rapidfuzz` when available.
   - Produces one master supplier ID per duplicate group.

5. `run_quality_checks`
   - Applies `configs/dq_rules.yaml`.
   - Writes DQ metrics with checked and failed record counts.

6. `build_gold_tables`
   - Produces mastered, analytics-ready, and ML-ready supplier tables.

7. `publish_metrics`
   - Exposes Gold tables for SQL dashboards and downstream consumers.

## Column-Level Movement

| Source examples | Canonical field | Silver enrichment | Gold usage |
| --- | --- | --- | --- |
| `company_name`, `legal_business_name`, `vendor_name` | `supplier_name` | `supplier_name_clean`, `supplier_name_match_key` | Supplier master and duplicate analysis |
| `contact_email`, `vendor_email`, `email_address` | `email` | `email_clean`, `email_is_valid` | DQ metrics and ML features |
| `phone_number`, `vendor_phone`, `phone` | `phone` | `phone_digits`, `phone_is_valid` | Dedupe exact match and ML features |
| `category`, `vendor_category`, `category_name` | `category` | `category_standardized` | Category analytics |
| `annual_spend` | `annual_spend` | `annual_spend_amount` | Spend summary |
