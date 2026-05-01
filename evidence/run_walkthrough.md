# SourceIQ End-to-End Run Evidence

This run includes the Kaggle Procurement KPI Analysis Dataset as `kaggle_procurement_kpi`.

Validation: the project is covered by GitHub Actions CI running `pytest -q` on every push.

## Layer Counts

| layer | table | rows | columns |
| --- | --- | --- | --- |
| bronze | conference_csv | 5 | 15 |
| bronze | government_vendor | 4 | 16 |
| bronze | erp_export | 4 | 16 |
| bronze | supplier_activity_api | 4 | 14 |
| bronze | kaggle_procurement_kpi | 777 | 16 |
| silver | supplier_standardized | 794 | 55 |
| silver | supplier_deduplicated | 794 | 61 |
| gold | gold_supplier_master | 11 | 22 |
| gold | gold_supplier_quality_metrics | 7 | 9 |
| gold | gold_supplier_spend_summary | 11 | 4 |
| gold | gold_supplier_category_analytics | 15 | 6 |
| gold | gold_ml_supplier_features | 11 | 9 |

## Kaggle Procurement Sample After Standardization

| supplier_name_clean | category_standardized | order_date | delivery_date | order_status_clean | quantity_ordered | negotiated_price_amount | annual_spend_amount | defective_units_count | compliance_status_clean | risk_signal | completeness_score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Alpha Inc | Electronics | 2022-01-01 | 2022-01-12 | Delivered | 552.0 | 18.79 | 10372.08 | 13.0 | Yes | medium | 1.0 |
| Alpha Inc | Raw Materials | 2022-01-01 | 2022-01-07 | Delivered | 1042.0 | 35.78 | 37282.76 | 33.0 | Yes | medium | 1.0 |
| Delta Logistics | Packaging | 2022-01-02 | 2022-01-06 | Delivered | 85.0 | 20.68 | 1757.8 | 6.0 | Yes | medium | 1.0 |
| Alpha Inc | Packaging | 2022-01-03 | 2022-01-20 | Delivered | 172.0 | 55.92 | 9618.24 | 4.0 | Yes | medium | 1.0 |
| Gamma Co | Packaging | 2022-01-04 | 2022-01-12 | Delivered | 456.0 | 77.86 | 35504.159999999996 | 26.0 | Yes | medium | 1.0 |
| Alpha Inc | Electronics | 2022-01-04 | 2022-01-06 | Delivered | 1004.0 | 46.59 | 46776.36 | 27.0 | Yes | medium | 1.0 |
| Epsilon Group | Electronics | 2022-01-06 | 2022-01-14 | Delivered | 609.0 | 82.75 | 50394.75 | 29.0 | Yes | medium | 1.0 |
| Alpha Inc | Office Supplies | 2022-01-07 | 2022-01-15 | Delivered | 1234.0 | 15.66 | 19324.44 | 26.0 | Yes | medium | 1.0 |
| Epsilon Group | Maintenance, Repair, and Operations | 2022-01-07 | 2022-01-26 | Pending | 739.0 | 66.09 | 48840.51 | 19.0 | Yes | medium | 1.0 |
| Gamma Co | Electronics | 2022-01-08 | 2022-01-13 | Delivered | 1579.0 | 43.41 | 68544.39 | 74.0 | Yes | medium | 1.0 |
| Epsilon Group | Packaging | 2022-01-09 | 2022-01-19 | Delivered | 148.0 | 85.96 | 12722.08 | 4.0 | Yes | medium | 1.0 |
| Epsilon Group | Packaging | 2022-01-10 | 2022-01-21 | Pending | 487.0 | 10.63 | 5176.81 | 15.0 | Yes | medium | 1.0 |
| Beta Supplies | Maintenance, Repair, and Operations | 2022-01-10 | 2022-01-20 | Delivered | 1306.0 | 20.17 | 26342.02 | 116.0 | No | high | 1.0 |
| Epsilon Group | Office Supplies | 2022-01-12 | 2022-01-17 | Delivered | 1563.0 | 70.8 | 110660.4 | 41.0 | Yes | medium | 1.0 |
| Gamma Co | Packaging | 2022-01-16 | 2022-01-22 | Delivered | 1203.0 | 47.68 | 57359.04 | 49.0 | Yes | medium | 1.0 |
| Epsilon Group | Packaging | 2022-01-20 | 2022-01-30 | Partially Delivered | 844.0 | 25.08 | 21167.519999999997 | 38.0 | Yes | medium | 1.0 |
| Beta Supplies | Packaging | 2022-01-20 | 2022-02-03 | Pending | 810.0 | 66.8 | 54108.0 | 73.0 | Yes | medium | 1.0 |
| Beta Supplies | Packaging | 2022-01-21 |  | Delivered | 1548.0 | 22.87 | 35402.76 |  | Yes | low | 1.0 |
| Beta Supplies | Raw Materials | 2022-01-21 |  | Delivered | 1058.0 | 57.21 | 60528.18 | 101.0 | Yes | high | 1.0 |
| Gamma Co | Raw Materials | 2022-01-22 | 2022-01-24 | Delivered | 1693.0 | 49.84 | 84379.12000000001 | 88.0 | Yes | medium | 1.0 |

## Duplicate Groups Sample

| supplier_name_clean | source_system | dedupe_key | duplicate_record_count | duplicate_score | record_rank | is_master_record | annual_spend_amount | risk_signal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 1 | True | 20944.559999999998 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 2 | False | 35861.32 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 3 | False | 57421.799999999996 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 4 | False | 19324.44 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 5 | False | 60625.770000000004 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 6 | False | 3065.78 | low |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 7 | False | 19731.25 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 8 | False | 11282.88 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 9 | False | 123293.73999999999 | high |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 10 | False | 54508.52 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 11 | False | 2658.96 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 12 | False | 57382.0 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 13 | False | 25322.82 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 14 | False | 47230.68 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 15 | False | 51374.75 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 16 | False | 8223.12 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 17 | False | 120772.8 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 18 | False | 50424.0 | high |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 19 | False | 100577.61 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 20 | False | 69427.35 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 21 | False | 13799.400000000001 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 22 | False | 16184.480000000001 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 23 | False | 66556.95 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 24 | False | 86526.78 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 25 | False | 16856.22 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 26 | False | 26983.04 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 27 | False | 87308.07 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 28 | False | 32213.61 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 29 | False | 37945.5 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 30 | False | 43717.479999999996 | low |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 31 | False | 53044.26 | high |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 32 | False | 1933.84 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 33 | False | 31342.56 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 34 | False | 10372.08 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 35 | False | 43253.28 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 36 | False | 13662.0 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 37 | False | 28494.1 | medium |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 38 | False | 111710.40000000001 | high |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 39 | False | 17466.4 | high |
| Alpha Inc | kaggle_procurement_kpi | name:ALPHA | 141 | 100.0 | 40 | False | 49190.72 | high |

## Master Suppliers

| supplier_name | category_standardized | state_clean | source_record_count | source_system_count | duplicate_record_count | avg_completeness_score | activity_count | risk_signal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Abc Technologies | IT Services | NY | 5 | 4 | 5 | 0.925 | 17.0 |  |
| Acme Medical Devices | Medical Devices | MA | 2 | 2 | 2 | 0.9375 |  |  |
| Alpha Inc | Office Supplies |  | 141 | 1 | 141 | 1.0 |  | medium |
| Bad Email Partners | Professional Services | CO | 2 | 2 | 2 | 0.875 | 3.0 |  |
| Beta Supplies | Packaging |  | 156 | 1 | 156 | 1.0 |  | high |
| Delta Logistics | Office Supplies |  | 171 | 1 | 171 | 1.0 |  | high |
| Epsilon Group | Maintenance, Repair, and Operations |  | 166 | 1 | 166 | 1.0 |  | medium |
| Evergreen Office Supplies Inc | Office Supplies | IL | 3 | 3 | 3 | 0.8333333333333334 | 6.0 |  |
| Gamma Co | Maintenance, Repair, and Operations |  | 143 | 1 | 143 | 1.0 |  | medium |
| Invalid Tax Co | Professional Services | TX | 1 | 1 | 1 | 1.0 |  |  |
| North Wind Industrial Llc | Industrial Supplies | CA | 4 | 4 | 4 | 0.90625 | 9.0 |  |

## Spend Summary

| supplier_name | total_annual_spend | primary_currency | currency_count |
| --- | --- | --- | --- |
| Beta Supplies | 9858665.900000002 |  | 0 |
| Epsilon Group | 9851156.060000002 |  | 0 |
| Delta Logistics | 9236240.469999999 |  | 0 |
| Gamma Co | 8587921.709999997 |  | 0 |
| Alpha Inc | 7839712.250000003 |  | 0 |
| Abc Technologies | 124500.22 | USD | 1 |
| Acme Medical Devices | 98200.0 | USD | 1 |
| North Wind Industrial Llc | 78250.5 | USD | 1 |
| Evergreen Office Supplies Inc | 20110.8 | USD | 1 |
| Invalid Tax Co | 0.0 |  | 0 |
| Bad Email Partners | 0.0 |  | 0 |

## ML Supplier Features

| supplier_name | has_valid_email | has_valid_phone | avg_completeness_score | duplicate_record_count | source_system_count | total_annual_spend | activity_count | has_high_risk_signal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Abc Technologies | 1.0 | 1.0 | 0.925 | 5 | 4 | 124500.22 | 17 | 0 |
| Acme Medical Devices | 1.0 | 1.0 | 0.9375 | 2 | 2 | 98200.0 | 0 | 0 |
| Alpha Inc |  |  | 1.0 | 141 | 1 | 7839712.250000003 | 0 | 1 |
| Bad Email Partners | 0.0 | 1.0 | 0.875 | 2 | 2 | 0.0 | 3 | 1 |
| Beta Supplies |  |  | 1.0 | 156 | 1 | 9858665.900000002 | 0 | 1 |
| Delta Logistics |  |  | 1.0 | 171 | 1 | 9236240.469999999 | 0 | 1 |
| Epsilon Group |  |  | 1.0 | 166 | 1 | 9851156.060000002 | 0 | 1 |
| Evergreen Office Supplies Inc | 1.0 | 1.0 | 0.8333333333333334 | 3 | 3 | 20110.8 | 6 | 0 |
| Gamma Co |  |  | 1.0 | 143 | 1 | 8587921.709999997 | 0 | 1 |
| Invalid Tax Co | 1.0 | 0.0 | 1.0 | 1 | 1 | 0.0 | 0 | 0 |
| North Wind Industrial Llc | 1.0 | 1.0 | 0.90625 | 4 | 4 | 78250.5 | 9 | 0 |

## Data Quality Results

| rule_name | records_checked | records_failed | failure_percentage | quality_status |
| --- | --- | --- | --- | --- |
| completeness_minimum | 794 | 0 | 0.0 | pass |
| duplicate_score_below_review_threshold | 794 | 0 | 0.0 | pass |
| email_format_valid | 794 | 2 | 0.0025 | watch |
| phone_length_valid | 794 | 1 | 0.0013 | watch |
| supplier_name_not_null | 794 | 0 | 0.0 | pass |
| tax_id_format_valid | 794 | 8 | 0.0101 | watch |
| us_state_valid | 794 | 0 | 0.0 | pass |
