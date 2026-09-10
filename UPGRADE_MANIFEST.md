# Sentinel Upgrade Pack Manifest

## Replace in existing repository
- src/common/config.py (if your current version differs, merge paths carefully)
- src/common/data_quality.py
- src/jobs/bronze_ingestion.py
- src/jobs/silver_transform.py
- src/jobs/gold_marts.py
- .github/workflows/python-tests.yml (merge if you already have a richer workflow)

## Add
- src/common/logger.py
- src/common/audit.py
- src/common/watermark.py
- src/common/reconciliation.py
- src/common/delta_utils.py
- src/jobs/scd2_customer.py
- src/jobs/reconciliation.py
- src/jobs/data_quality_metrics.py
- src/jobs/run_pipeline.py
- tests/test_watermark.py
- tests/test_reconciliation.py
- tests/test_project_structure.py
- config/entities.yml
- docs/architecture.md
- docs/data_model.md
- docs/how_to_run.md

## README
`README_UPGRADE_SECTION.md` is a ready-to-use replacement/reference section. Merge it into README.md rather than leaving it as a second README.
