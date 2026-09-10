from pyspark.sql import SparkSession, functions as F


spark = (
    SparkSession.builder
    .appName("sentinel-bronze")
    .getOrCreate()
)


ENTITIES = [
    "customers",
    "brokers",
    "policies",
    "claims",
    "payments"
]


for entity in ENTITIES:

    print(f"Starting Bronze ingestion for: {entity}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"data/raw/{entity}.csv")
    )

    bronze_df = (
        df
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "_batch_id",
            F.date_format(
                F.current_timestamp(),
                "yyyyMMddHHmmss"
            )
        )
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(f"data/bronze/{entity}")
    )

    record_count = bronze_df.count()

    print(
        f"Bronze ingestion completed for {entity}. "
        f"Records ingested: {record_count}"
    )
