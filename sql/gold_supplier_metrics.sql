CREATE OR REPLACE VIEW gold_supplier_metrics AS
SELECT
  m.master_supplier_id,
  m.supplier_name,
  m.category_standardized,
  m.state_clean,
  m.source_record_count,
  m.source_system_count,
  m.duplicate_record_count,
  m.avg_completeness_score,
  COALESCE(s.total_annual_spend, 0) AS total_annual_spend,
  f.has_valid_email,
  f.has_valid_phone,
  f.has_high_risk_signal
FROM gold_supplier_master m
LEFT JOIN gold_supplier_spend_summary s
  ON m.master_supplier_id = s.master_supplier_id
LEFT JOIN gold_ml_supplier_features f
  ON m.master_supplier_id = f.master_supplier_id;
