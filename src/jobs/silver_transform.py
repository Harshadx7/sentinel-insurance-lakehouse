from pyspark.sql import SparkSession, Window, functions as F

from src.common.data_quality import (
    required,
    non_negative,
    status
)


spark = (
    SparkSession.builder
    .appName("sentinel-silver")
    .getOrCreate()
)


def latest(
    entity,
    key,
    required_cols,
    amount_cols=None
):

    if amount_cols is None:
        amount_cols = []

    print(f"Starting Silver transformation for: {entity}")

    df = (
        spark.read
        .format("delta")
        .load(f"data/bronze/{entity}")
    )

    # Convert source update timestamp
    if "updated_at" in df.columns:
        df = df.withColumn(
            "updated_at",
            F.to_timestamp("updated_at")
        )

        window_spec = (
            Window
            .partitionBy(key)
            .orderBy(
                F.col("updated_at").desc(),
                F.col("_ingested_at").desc()
            )
        )

    else:

        window_spec = (
            Window
            .partitionBy(key)
            .orderBy(
                F.col("_ingested_at").desc()
            )
        )

    # Deduplicate and retain latest record
    df = (
        df
        .withColumn(
            "_rn",
            F.row_number().over(window_spec)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Data quality checks
    df = required(df, required_cols)

    df = non_negative(
        df,
        amount_cols
    )

    df = status(df)

    valid = (
        df
        .filter(
            F.col("dq_status") == "VALID"
        )
    )

    invalid = (
        df
        .filter(
            F.col("dq_status") == "QUARANTINE"
        )
    )

    # Write valid records
    (
        valid.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"data/silver/{entity}")
    )

    # Write quarantined records
    (
        invalid.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"data/quarantine/{entity}")
    )

    print(
        f"{entity} completed | "
        f"Valid: {valid.count()} | "
        f"Quarantine: {invalid.count()}"
    )


latest(
    "customers",
    "customer_id",
    ["customer_id", "email"]
)

latest(
    "brokers",
    "broker_id",
    ["broker_id", "broker_name"]
)

latest(
    "policies",
    "policy_id",
    [
        "policy_id",
        "customer_id",
        "written_premium"
    ],
    [
        "written_premium"
    ]
)

latest(
    "claims",
    "claim_id",
    [
        "claim_id",
        "policy_id",
        "incurred_amount"
    ],
    [
        "incurred_amount",
        "fraud_score"
    ]
)

latest(
    "payments",
    "payment_id",
    [
        "payment_id",
        "claim_id",
        "payment_amount"
    ],
    [
        "payment_amount"
    ]
)
