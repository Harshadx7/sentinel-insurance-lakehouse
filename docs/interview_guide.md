# Interview Guide
## Problem
Insurance data is distributed across policy, claims, payment and broker systems. Reporting needs a trusted analytical model while operational data changes frequently.

## Solution
I designed a medallion lakehouse. Bronze preserves lineage, Silver validates and standardises records, and Gold exposes a dimensional model.

## Challenges I would discuss
1. Late-arriving claims and payments.
2. Duplicate source extracts.
3. Referential integrity between policy and claims.
4. Invalid monetary values.
5. Historical customer changes.
6. Reconciling operational counts and financial totals.

## If asked about scale
The generator is parameterized. Local defaults are intentionally small, but environment variables can increase customers, policies and claims for larger Spark testing.

## If asked what I would improve in production
Delta Change Data Feed, structured streaming, Great Expectations/Soda, Databricks Workflows, Azure Key Vault, Terraform/Bicep and CI/CD.
