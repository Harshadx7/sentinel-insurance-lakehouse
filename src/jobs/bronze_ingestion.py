from datetime import datetime
from pyspark.sql import functions as F
from src.common.config import load_entities
from src.common.spark import get_spark
from src.common.watermark import get_watermark,set_watermark
from src.common.audit import write_audit
spark=get_spark("sentinel-bronze-incremental")
run_id=datetime.utcnow().strftime("%Y%m%d%H%M%S")
for entity,cfg in load_entities().items():
    wm_col=cfg["watermark_column"]
    try:
        df=spark.read.option("header",True).option("inferSchema",True).csv(f"data/raw/{entity}.csv").withColumn(wm_col,F.to_timestamp(wm_col))
        previous=get_watermark(spark,entity)
        if previous is not None: df=df.filter(F.col(wm_col)>F.lit(previous).cast("timestamp"))
        count=df.count()
        if count==0:
            write_audit(spark,run_id,"bronze_ingestion",entity,"SUCCESS",0,0,message="No new records after watermark")
            continue
        batch=df.withColumn("_source_file",F.input_file_name()).withColumn("_ingested_at",F.current_timestamp()).withColumn("_batch_id",F.lit(run_id))
        batch.write.format("delta").mode("append").option("mergeSchema","true").save(f"data/bronze/{entity}")
        maximum=batch.agg(F.max(wm_col).alias("wm")).collect()[0]["wm"]
        set_watermark(spark,entity,maximum,run_id)
        write_audit(spark,run_id,"bronze_ingestion",entity,"SUCCESS",count,count,message=f"Watermark advanced to {maximum}")
    except Exception as exc:
        write_audit(spark,run_id,"bronze_ingestion",entity,"FAILED",message=str(exc)); raise
