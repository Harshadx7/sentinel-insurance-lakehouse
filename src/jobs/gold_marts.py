from pyspark.sql import SparkSession, functions as F
spark=SparkSession.builder.appName("sentinel-gold").getOrCreate()
c=spark.read.format("delta").load("data/silver/customers")
b=spark.read.format("delta").load("data/silver/brokers")
p=spark.read.format("delta").load("data/silver/policies")
cl=spark.read.format("delta").load("data/silver/claims")
pay=spark.read.format("delta").load("data/silver/payments")

dims={"dim_customer":c.select("customer_id","customer_name","email","region","date_of_birth"),
"dim_broker":b.select("broker_id","broker_name","region"),
"dim_policy":p.select("policy_id","customer_id","broker_id","product","policy_status","effective_date","expiry_date")}
facts={"fact_policy_premium":p.select("policy_id","written_premium","currency"),
"fact_claim":cl.select("claim_id","policy_id","reported_date","claim_status","incurred_amount","fraud_score"),
"fact_payment":pay.select("payment_id","claim_id","payment_date","payment_amount","payment_type")}
for n,d in {**dims,**facts}.items(): d.write.format("delta").mode("overwrite").save(f"data/gold/{n}")

mart=(p.join(cl,"policy_id","left")
 .groupBy("product","policy_status")
 .agg(F.sum("written_premium").alias("written_premium"),
      F.sum(F.coalesce("incurred_amount",F.lit(0))).alias("incurred_claims"),
      F.countDistinct("policy_id").alias("policy_count"),
      F.countDistinct("claim_id").alias("claim_count"),
      F.avg("fraud_score").alias("avg_fraud_score"))
 .withColumn("loss_ratio",F.round(F.col("incurred_claims")/F.nullif(F.col("written_premium"),F.lit(0)),4)))
mart.write.format("delta").mode("overwrite").save("data/gold/mart_insurance_kpi")
