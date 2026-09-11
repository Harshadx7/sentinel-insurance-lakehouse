from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable
WATERMARK_PATH="data/control/watermarks"
def get_watermark(spark,entity,path=WATERMARK_PATH):
    try:
        rows=(spark.read.format("delta").load(path).filter(F.col("entity")==entity)
              .orderBy(F.col("updated_at").desc()).limit(1).collect())
        return rows[0]["watermark_value"] if rows else None
    except Exception: return None
def set_watermark(spark,entity,value,batch_id,path=WATERMARK_PATH):
    if value is None: return
    source=spark.createDataFrame([(entity,str(value),batch_id,datetime.utcnow())],["entity","watermark_value","batch_id","updated_at"])
    try:
        target=DeltaTable.forPath(spark,path)
        target.alias("t").merge(source.alias("s"),"t.entity=s.entity").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception:
        source.write.format("delta").mode("append").save(path)
