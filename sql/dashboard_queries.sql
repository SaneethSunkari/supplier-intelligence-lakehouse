-- Page 1: Supplier data quality overview
SELECT
  rule_name,
  severity,
  records_checked,
  records_failed,
  failure_percentage,
  quality_status,
  run_timestamp
FROM gold_supplier_quality_metrics
ORDER BY failure_percentage DESC, severity;

-- Page 2: Duplicate supplier analysis
SELECT
  supplier_name,
  category_standardized,
  state_clean,
  source_record_count,
  duplicate_record_count,
  source_system_count,
  avg_completeness_score
FROM gold_supplier_master
WHERE duplicate_record_count > 1
ORDER BY duplicate_record_count DESC, supplier_name;

-- Page 3: Supplier distribution by state and category
SELECT
  state_clean,
  category_standardized,
  supplier_count,
  total_annual_spend,
  avg_completeness_score
FROM gold_supplier_category_analytics
ORDER BY supplier_count DESC, total_annual_spend DESC;

-- Page 4: Spend by supplier
SELECT
  m.supplier_name,
  m.category_standardized,
  m.state_clean,
  s.total_annual_spend,
  s.primary_currency
FROM gold_supplier_master m
JOIN gold_supplier_spend_summary s
  ON m.master_supplier_id = s.master_supplier_id
ORDER BY s.total_annual_spend DESC;

-- Page 5: Pipeline run health
SELECT
  batch_id,
  COUNT(*) AS supplier_records,
  COUNT(DISTINCT master_supplier_id) AS mastered_suppliers,
  AVG(avg_completeness_score) AS avg_completeness_score,
  MAX(duplicate_record_count) AS max_duplicate_record_count
FROM gold_supplier_master
GROUP BY batch_id
ORDER BY batch_id DESC;
