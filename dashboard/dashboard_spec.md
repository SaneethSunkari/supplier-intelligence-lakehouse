# Dashboard Specification

## Tool Options

The Gold tables can be served to Databricks SQL, Power BI, Tableau, DuckDB, or any SQL client that can read the local lakehouse outputs.

## Pages

### Supplier Data Quality Overview

- KPI cards: total checks, failed checks, average failure percentage.
- Bar chart: failure percentage by rule.
- Table: rule name, severity, checked records, failed records, latest run timestamp.

### Duplicate Supplier Analysis

- KPI cards: duplicate groups, duplicate records, highest duplicate group size.
- Table: supplier name, duplicate count, source count, category, state.

### Supplier Distribution

- Map or matrix: supplier count by state.
- Bar chart: supplier count by category.
- Matrix: state by category with average completeness.

### Spend Analytics

- Bar chart: annual spend by supplier.
- Bar chart: annual spend by category.
- Table: supplier, category, state, spend, currency.

### Pipeline Run Health

- Trend: mastered suppliers by batch.
- KPI cards: average completeness, max duplicate group size.
- Table: batch ID and source record counts.
