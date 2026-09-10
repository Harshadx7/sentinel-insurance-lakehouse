# Sentinel Insurance Lakehouse
## Enterprise Claims, Policy & Fraud Analytics Platform

> **Flagship Data Engineering Portfolio Project**

A production-inspired end-to-end insurance data platform demonstrating batch ingestion, medallion architecture, incremental processing, data quality, SCD Type 2, Delta Lake, dbt-style warehouse modelling, reconciliation and BI-ready analytics.

## Executive summary
Sentinel is a fictional global insurer. Operational systems generate customer, policy, broker, claim and payment data. The platform ingests these sources into a Bronze layer, cleans and validates them in Silver, and publishes dimensional Gold marts for analytics.

**Business outcomes**
- Written premium and policy growth
- Incurred claims and loss ratio
- Claim settlement cycle time
- Broker and regional performance
- High-risk fraud triage
- Source-to-target reconciliation

## Architecture
```text
Synthetic Operational Systems
CSV / API / Database simulation
            |
            v
 Azure Data Factory (orchestration pattern)
            |
            v
      ADLS GEN2 - BRONZE
 Raw data + source metadata + batch audit
            |
            v
 Azure Databricks / PySpark
 Deduplication + validation + standardisation
            |
            v
      DELTA LAKE - SILVER
 Clean, conformed, quarantine records
            |
            v
 Delta MERGE / SCD Type 2 / Business rules
            |
            v
        GOLD LAYER
 Dimensions + Facts + KPI marts
            |
       +----+-----+
       |          |
       v          v
 Snowflake/dbt   Power BI/Tableau
```

## Technology stack
Azure Data Factory | ADLS Gen2 | Azure Databricks | PySpark | Delta Lake | Python | SQL | dbt | Snowflake | Power BI/Tableau

## Data model
**Dimensions:** dim_customer, dim_policy, dim_broker, dim_date  
**Facts:** fact_policy_premium, fact_claim, fact_payment  
**Mart:** mart_insurance_kpi

## Engineering capabilities demonstrated
- Bronze / Silver / Gold architecture
- Incremental ingestion and watermark pattern
- Idempotent processing
- Delta Lake MERGE
- SCD Type 2 history
- PySpark window functions
- Data quality checks and quarantine
- Source-to-target reconciliation
- Star schema modelling
- SQL CTEs and window functions
- dbt transformations and tests
- Parameterized orchestration design

## Quick start
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.generators.generate_data
```

For Spark:
```bash
spark-submit src/jobs/bronze_ingestion.py
spark-submit src/jobs/silver_transform.py
spark-submit src/jobs/gold_marts.py
```

## Portfolio disclaimer
All data is synthetically generated. No client, employer, policyholder or production data is included.
