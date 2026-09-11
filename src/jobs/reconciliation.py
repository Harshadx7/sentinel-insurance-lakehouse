from datetime import datetime
from src.common.config import load_entities
from src.common.reconciliation import reconcile_counts
from src.common.spark import get_spark
spark=get_spark("sentinel-reconciliation"); run_id=datetime.utcnow().strftime("%Y%m%d%H%M%S")
rows=[]
for entity in load_entities():
    r=reconcile_counts(spark,entity); r["run_id"]=run_id; r["run_timestamp"]=datetime.utcnow(); rows.append(r)
spark.createDataFrame(rows).write.format("delta").mode("append").save("data/control/reconciliation_results")
