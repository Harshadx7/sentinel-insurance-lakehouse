# Sentinel Insurance Lakehouse

**Production-inspired end-to-end Data Engineering Lakehouse portfolio project**

Sentinel Insurance Lakehouse demonstrates an end-to-end insurance data platform built using **PySpark, Delta Lake and the Medallion architecture**.

The project processes synthetic insurance data through Bronze, Silver and Gold layers while demonstrating incremental ingestion, data quality, Delta MERGE upserts, SCD Type 2 history, audit logging, reconciliation and automated testing.

—

## What this project demonstrates

### Production-inspired engineering capabilities

- **Configuration-driven ingestion:** entity metadata, business keys and watermark columns are defined through YAML configuration.
- **Synthetic insurance data generation:** linked customer, broker, policy, claim and payment datasets.
- **Incremental Bronze ingestion:** persisted per-entity watermarks process only records newer than the last successful watermark.
- **Silver deduplication:** records are reduced to the latest version at business-key grain.
- **Data quality validation:** required-field and non-negative validations identify invalid records.
- **Quarantine routing:** invalid records are separated from valid Silver datasets.
- **Delta MERGE upserts:** valid Silver records are maintained using business-key-based Delta MERGE operations.
- **Explicit audit schema:** pipeline execution metrics are persisted using a defined Delta audit schema.
- **Customer SCD Type 2:** historical customer changes are preserved with effective dates and a current-record flag.
- **Gold dimensional model:** dimensions and facts are published for analytics.
- **Insurance KPI mart:** premiums, claims, policy counts, claim counts, fraud metrics and loss ratios.
- **Source-to-target reconciliation:** Bronze records are reconciled at the latest deduplicated business-key grain.
- **Automated testing:** data quality, watermark and reconciliation behaviour are validated with pytest.
- **GitHub Actions CI:** automated tests run on pushes and pull requests.

—

## Architecture

```text
                         ┌───────────────────────┐
                         │ Synthetic CSV Sources │
                         └───────────┬───────────┘
                                     │
                                     ▼
                     ┌───────────────────────────┐
                     │     Bronze Ingestion      │
                     │ Incremental + Watermarks  │
                     └───────────┬───────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │      Bronze Delta       │
                    └────────────┬────────────┘
                                 │
                                 ▼
                  ┌─────────────────────────────┐
                  │     Silver Transformation   │
                  │ Deduplication + DQ + MERGE  │
                  └─────────────┬───────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
             ┌─────────────┐         ┌─────────────┐
             │   Silver    │         │ Quarantine  │
             │ Valid Data  │         │ Invalid DQ  │
             └──────┬──────┘         └──────┬──────┘
                    │                       │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │ Reconciliation + Audit │
                    └────────────┬───────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │       Gold Layer       │
                    │ Dimensions + Facts     │
                    │ Insurance KPI Mart     │
                    └────────────────────────┘
Data Flow
How to Run
Project Structure
Key Engineering Decisions
Testing
Interview Summary 