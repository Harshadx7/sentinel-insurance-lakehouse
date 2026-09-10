from pyspark.sql import SparkSession, functions as F


spark = (
    SparkSession.builder
    .appName("sentinel-gold")
    .getOrCreate()
)


# --------------------------------------------------
# Read Silver Layer
# --------------------------------------------------

customers = (
    spark.read
    .format("delta")
    .load("data/silver/customers")
)

brokers = (
    spark.read
    .format("delta")
    .load("data/silver/brokers")
)

policies = (
    spark.read
    .format("delta")
    .load("data/silver/policies")
)

claims = (
    spark.read
    .format("delta")
    .load("data/silver/claims")
)

payments = (
    spark.read
    .format("delta")
    .load("data/silver/payments")
)


# --------------------------------------------------
# Dimension Tables
# --------------------------------------------------

dimensions = {

    "dim_customer": (
        customers.select(
            "customer_id",
            "customer_name",
            "email",
            "region",
            "date_of_birth"
        )
    ),

    "dim_broker": (
        brokers.select(
            "broker_id",
            "broker_name",
            "region"
        )
    ),

    "dim_policy": (
        policies.select(
            "policy_id",
            "customer_id",
            "broker_id",
            "product",
            "policy_status",
            "effective_date",
            "expiry_date"
        )
    )
}


# --------------------------------------------------
# Fact Tables
# --------------------------------------------------

facts = {

    "fact_policy_premium": (
        policies.select(
            "policy_id",
            "written_premium",
            "currency"
        )
    ),

    "fact_claim": (
        claims.select(
            "claim_id",
            "policy_id",
            "reported_date",
            "claim_status",
            "incurred_amount",
            "fraud_score"
        )
    ),

    "fact_payment": (
        payments.select(
            "payment_id",
            "claim_id",
            "payment_date",
            "payment_amount",
            "payment_type"
        )
    )
}


# --------------------------------------------------
# Write Dimensions and Facts
# --------------------------------------------------

for name, dataframe in {
    **dimensions,
    **facts
}.items():

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"data/gold/{name}")
    )

    print(
        f"Gold table created: {name}"
    )


# --------------------------------------------------
# Aggregate Claims BEFORE Joining to Policies
# --------------------------------------------------

claims_by_policy = (

    claims
    .groupBy("policy_id")
    .agg(

        F.sum(
            "incurred_amount"
        ).alias(
            "incurred_claims"
        ),

        F.countDistinct(
            "claim_id"
        ).alias(
            "claim_count"
        ),

        F.avg(
            "fraud_score"
        ).alias(
            "avg_fraud_score"
        )
    )
)


# --------------------------------------------------
# Insurance KPI Mart
# --------------------------------------------------

mart = (

    policies

    .join(
        claims_by_policy,
        "policy_id",
        "left"
    )

    .groupBy(
        "product",
        "policy_status"
    )

    .agg(

        F.sum(
            "written_premium"
        ).alias(
            "written_premium"
        ),

        F.sum(
            F.coalesce(
                F.col("incurred_claims"),
                F.lit(0)
            )
        ).alias(
            "incurred_claims"
        ),

        F.countDistinct(
            "policy_id"
        ).alias(
            "policy_count"
        ),

        F.sum(
            F.coalesce(
                F.col("claim_count"),
                F.lit(0)
            )
        ).alias(
            "claim_count"
        ),

        F.avg(
            "avg_fraud_score"
        ).alias(
            "avg_fraud_score"
        )
    )

    .withColumn(

        "loss_ratio",

        F.round(

            F.col(
                "incurred_claims"
            )

            /

            F.when(
                F.col(
                    "written_premium"
                ) != 0,

                F.col(
                    "written_premium"
                )

            ).otherwise(
                F.lit(None)
            ),

            4
        )
    )
)


# --------------------------------------------------
# Write KPI Mart
# --------------------------------------------------

(
    mart.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(
        "data/gold/mart_insurance_kpi"
    )
)


print(
    "Gold insurance KPI mart created successfully"
)
