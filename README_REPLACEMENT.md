# Sentinel Insurance Lakehouse

A production-inspired PySpark and Delta Lake portfolio project for insurance data engineering.

## Implemented capabilities
- YAML configuration-driven entities
- Synthetic insurance data generation
- Incremental Bronze ingestion with persisted watermarks
- Silver deduplication, data quality and Delta MERGE upserts
- Quarantine routing
- Pipeline audit logging
- Gold dimensional model and insurance KPI mart
- Customer SCD Type 2 history
- Source-to-target reconciliation
- Automated repository tests and GitHub Actions CI

See `docs/architecture.md`, `docs/data_model.md` and `docs/how_to_run.md`.

## Interview story
“I built a production-inspired insurance lakehouse using PySpark and Delta Lake. Entity metadata is driven by YAML configuration. Bronze ingestion tracks per-entity watermarks for incremental processing. Silver deduplicates records, applies data-quality rules and uses Delta MERGE for upserts, routing invalid records to quarantine. Gold publishes dimensions, facts and insurance KPIs. I also implemented customer SCD Type 2 history, audit logging, reconciliation and CI-based tests.”
