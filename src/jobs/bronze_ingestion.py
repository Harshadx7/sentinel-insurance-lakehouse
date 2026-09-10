from pyspark.sql import SparkSession, functions as F
spark=SparkSession.builder.appName("sentinel-bronze").getOrCreate()
for entity in ["customers","brokers","policies","claims","payments"]:
    df=spark.read.option("header",True).option("inferSchema",True).csv(f"data/raw/{entity}.csv")
    df=(df.withColumn("_source_file",F.input_file_name())
          .withColumn("_ingested_at",F.current_timestamp())
          .withColumn("_batch_id",F.date_format(F.current_timestamp(),"yyyyMMddHHmmss")))
    df.write.format("delta").mode("append").option("mergeSchema","true").save(f"data/bronze/{entity}")
    print(entity, df.count())
