from pathlib import Path
def test_required_files_exist():
    for p in ["src/jobs/bronze_ingestion.py","src/jobs/silver_transform.py","src/jobs/gold_marts.py","src/jobs/scd2_customer.py","src/jobs/reconciliation.py","config/entities.yml"]:
        assert Path(p).exists(),p
