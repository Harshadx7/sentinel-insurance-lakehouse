from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import Window,functions as F
from src.common.config import load_entities
from src.common.data_quality import required,non_negative,status
from src.common.spark import get_spark
from src.common.audit import write_audit
spark=get_spark("sentinel-silver-merge")
run_id=datetime.utcnow().strftime("%Y%m%d%H%M%S")
for entity,cfg in load_entities().items():
    key=cfg["key"]
    try:
        df=spark.read.format("delta").load(f"data/bronze/{entity}")
        if "updated_at" in df.columns:
            df=df.withColumn("updated_at",F.to_timestamp("updated_at"))
            order=[F.col("updated_at").desc(),F.col("_ingested_at").desc()]
        else: order=[F.col("_ingested_at").desc()]
        latest=df.withColumn("_rn",F.row_number().over(Window.partitionBy(key).orderBy(*order))).filter("_rn=1").drop("_rn")
        checked=status(non_negative(required(latest,cfg.get("required_columns",[])),cfg.get("amount_columns",[])))
        valid=checked.filter("dq_status='VALID'"); invalid=checked.filter("dq_status='QUARANTINE'")
        path=f"data/silver/{entity}"
        try:
            target=DeltaTable.forPath(spark,path)
            target.alias("t").merge(valid.alias("s"),f"t.{key}=s.{key}").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        except Exception:
            valid.write.format("delta").mode("overwrite").save(path)
        invalid.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(f"data/quarantine/{entity}")
        write_audit(spark,run_id,"silver_transform",entity,"SUCCESS",latest.count(),valid.count(),invalid.count())
    except Exception as exc:
        write_audit(spark,run_id,"silver_transform",entity,"FAILED",message=str(exc)); raise
