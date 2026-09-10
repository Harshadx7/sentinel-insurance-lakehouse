from pyspark.sql import SparkSession, Window, functions as F
from src.common.data_quality import required, non_negative, status
spark=SparkSession.builder.appName("sentinel-silver").getOrCreate()

def latest(entity,key,required_cols,amount_cols=[]):
    df=spark.read.format("delta").load(f"data/bronze/{entity}")
    w=Window.partitionBy(key).orderBy(F.col("_ingested_at").desc())
    df=df.withColumn("_rn",F.row_number().over(w)).filter("_rn=1").drop("_rn")
    df=status(non_negative(required(df,required_cols),amount_cols))
    valid=df.filter("dq_status='VALID'")
    invalid=df.filter("dq_status='QUARANTINE'")
    valid.write.format("delta").mode("overwrite").save(f"data/silver/{entity}")
    invalid.write.format("delta").mode("overwrite").save(f"data/quarantine/{entity}")

latest("customers","customer_id",["customer_id","email"])
latest("brokers","broker_id",["broker_id","broker_name"])
latest("policies","policy_id",["policy_id","customer_id","written_premium"],["written_premium"])
latest("claims","claim_id",["claim_id","policy_id","incurred_amount"],["incurred_amount","fraud_score"])
latest("payments","payment_id",["payment_id","claim_id","payment_amount"],["payment_amount"])
