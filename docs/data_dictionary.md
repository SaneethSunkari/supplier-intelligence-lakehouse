# Data Dictionary

## Canonical Supplier Fields

| Field | Description |
| --- | --- |
| `supplier_name` | Source-provided supplier name. |
| `supplier_name_clean` | Trimmed, display-friendly supplier name. |
| `supplier_name_normalized` | Uppercase supplier name with punctuation and legal suffixes removed. |
| `supplier_name_match_key` | Compact name key for blocking and fuzzy matching. |
| `email_clean` | Lowercased email address. |
| `email_is_valid` | Boolean email validation result. |
| `phone_digits` | Ten-digit normalized phone number. |
| `phone_is_valid` | Boolean phone validation result. |
| `address_clean` | Cleaned street address. |
| `city_clean` | Standardized city. |
| `state_clean` | Two-letter state code where available. |
| `country_clean` | Standardized country code. |
| `category_standardized` | Business category after alias mapping. |
| `naics_code_clean` | Numeric NAICS code. |
| `tax_id_clean` | Standardized tax identifier. |
| `tax_id_is_valid` | Boolean tax ID validation result. |
| `annual_spend_amount` | Numeric supplier spend from ERP-style data. |
| `activity_count` | Supplier activity count from API-style data. |
| `risk_signal` | Activity API risk indicator. |
| `completeness_score` | Ratio of populated core fields. |
| `po_id` | Purchase order identifier from procurement transaction sources. |
| `order_date` | Purchase order date. |
| `delivery_date` | Supplier delivery date. |
| `delivery_days` | Days between order and delivery. |
| `order_status_clean` | Standardized purchase order status. |
| `quantity_ordered` | Numeric purchase order quantity. |
| `negotiated_price_amount` | Negotiated unit price from procurement transactions. |
| `annual_spend_amount` | Spend amount from ERP spend or quantity multiplied by negotiated/unit price. |
| `defective_units_count` | Defective unit count from supplier performance data. |
| `compliance_status_clean` | Supplier compliance status from procurement transactions. |

## DQ Metrics

| Field | Description |
| --- | --- |
| `dq_run_id` | Unique quality run ID. |
| `table_name` | Validated table. |
| `rule_name` | Configured rule name. |
| `records_checked` | Number of records checked by the rule. |
| `records_failed` | Number of records that failed. |
| `failure_percentage` | Failed records divided by checked records. |
| `run_timestamp` | UTC run timestamp. |
| `severity` | Business severity from config. |

## Mastering Fields

| Field | Description |
| --- | --- |
| `master_supplier_id` | Stable hashed ID based on dedupe key. |
| `dedupe_key` | Blocking key used to group duplicate records. |
| `duplicate_record_count` | Number of source records in the mastered group. |
| `duplicate_score` | Exact or fuzzy confidence score. |
| `record_rank` | Survivorship rank inside a duplicate group. |
| `is_master_record` | True for the selected survivor record. |
