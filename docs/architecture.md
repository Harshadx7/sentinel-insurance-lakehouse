# Architecture decisions
## Bronze
Immutable/raw representation with source file, batch ID and ingestion timestamp.

## Silver
Business-independent conformance layer. Deduplicates latest records using window functions, applies mandatory-field and non-negative validations, and writes failed rows to quarantine.

## Gold
Consumer-oriented star schema and aggregated KPI marts.

## Incremental pattern
In production, ADF obtains a source watermark, copies only records newer than the previous successful watermark, and persists the new watermark after successful downstream processing.

## SCD Type 2
For attributes such as customer region, production implementations maintain effective_from, effective_to and is_current. This repository documents the pattern as an extension because generated sample data is intentionally compact.
