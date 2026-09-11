from delta.tables import DeltaTable
from pyspark.sql import functions as F
from src.common.spark import get_spark
spark=get_spark("sentinel-scd2-customer")
src=spark.read.format("delta").load("data/silver/customers").select("customer_id","customer_name","email","region","date_of_birth")
attrs=["customer_name","email","region","date_of_birth"]
src=src.withColumn("_hash",F.sha2(F.concat_ws("||",*[F.coalesce(F.col(c).cast("string"),F.lit("")) for c in attrs]),256))
path="data/gold/dim_customer_scd2"
try:
    target=DeltaTable.forPath(spark,path); current=target.toDF().filter("is_current=true")
    changed=(src.alias("s").join(current.alias("t"),"customer_id","left").filter(F.col("t.customer_id").isNull()|(F.col("s._hash")!=F.col("t._hash"))).select("s.*"))
    ids=changed.select("customer_id").distinct()
    target.alias("t").merge(ids.alias("s"),"t.customer_id=s.customer_id AND t.is_current=true").whenMatchedUpdate(set={"is_current":"false","effective_to":"current_timestamp()"}).execute()
    changed.withColumn("effective_from",F.current_timestamp()).withColumn("effective_to",F.lit(None).cast("timestamp")).withColumn("is_current",F.lit(True)).write.format("delta").mode("append").save(path)
except Exception:
    src.withColumn("effective_from",F.current_timestamp()).withColumn("effective_to",F.lit(None).cast("timestamp")).withColumn("is_current",F.lit(True)).write.format("delta").mode("overwrite").save(path)
