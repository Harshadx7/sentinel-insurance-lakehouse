def reconcile_counts(spark,entity):
    bronze=spark.read.format("delta").load(f"data/bronze/{entity}").count()
    silver=spark.read.format("delta").load(f"data/silver/{entity}").count()
    quarantine=spark.read.format("delta").load(f"data/quarantine/{entity}").count()
    return {"entity":entity,"bronze_count":bronze,"silver_count":silver,"quarantine_count":quarantine,
            "difference":bronze-(silver+quarantine),"status":"PASS" if bronze>=silver+quarantine else "CHECK"}
