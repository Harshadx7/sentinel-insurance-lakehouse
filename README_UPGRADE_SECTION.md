# Sentinel Insurance Lakehouse

**Production-inspired end-to-end Data Engineering Lakehouse portfolio project**

## What this project demonstrates

This project implements an insurance analytics lakehouse using PySpark and Delta Lake with a Bronze, Silver and Gold architecture.

### Production-inspired engineering capabilities

- **Incremental ingestion:** persisted per-entity watermarks process only records newer than the last successful watermark.
- **Delta MERGE upserts:** Silver tables use business-key-based Delta MERGE instead of full overwrite.
- **SCD Type 2:** customer history is preserved with effective dates and a current-record flag.
- **Source-to-target reconciliation:** record counts and financial amounts are reconciled across Bronze, Silver and Quarantine.
- **Data quality:** required-field and non-negative validations route invalid records to Quarantine.
- **Auditability:** pipeline runs, reconciliation results and quality metrics are persisted as Delta audit datasets.
- **Configuration-driven ingestion:** entity keys and watermark columns are documented in `config/entities.yml`.
- **Logging and error visibility:** pipeline modules emit structured runtime logs.
- **Automated tests and CI:** lightweight tests run through GitHub Actions.

## Architecture

See [`docs/architecture.md`](docs/architecture.md).

## Data Flow

```text
Source
  ↓
Incremental Bronze Ingestion
  ↓        ↘
Bronze     Watermark Control
  ↓
Silver Validation + Delta MERGE ──→ Quarantine
  ↓                 ↓
SCD Type 2       Reconciliation
  ↓                 ↓
Gold Facts/Dimensions     Audit Tables
  ↓
Analytics / BI
```

## Project Structure

```text
src/common/        Shared configuration, logging, watermarks, reconciliation and Delta utilities
src/jobs/          Bronze, Silver, SCD2, reconciliation, quality metrics and orchestration jobs
tests/             Automated tests
config/            Entity configuration
docs/              Architecture, data model and run documentation
adf/               Orchestration design
dbt/               dbt models
sql/               Analytical SQL
```

## How to Run

See [`docs/how_to_run.md`](docs/how_to_run.md).

## Expected Results

After a successful run, the project produces:

- Bronze Delta datasets with ingestion metadata.
- A persisted watermark control dataset.
- Silver Delta datasets maintained through MERGE.
- Quarantine datasets for failed quality rules.
- `dim_customer_scd2` with historical customer versions.
- Gold facts, dimensions and KPI marts.
- Pipeline audit records.
- Data-quality metrics.
- Source-to-target reconciliation results with PASS/FAIL status.

## Important Portfolio Note

This is a **personal, production-inspired portfolio project using synthetic insurance data**. It is not a representation of confidential employer or client systems.
