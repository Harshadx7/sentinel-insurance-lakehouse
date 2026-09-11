# How to Run
```bash
pip install -r requirements.txt
python -m src.generators.generate_data
spark-submit src/jobs/bronze_ingestion.py
spark-submit src/jobs/silver_transform.py
spark-submit src/jobs/gold_marts.py
spark-submit src/jobs/scd2_customer.py
spark-submit src/jobs/reconciliation.py
pytest -q
```
Delta Lake must be available in the Spark runtime.
