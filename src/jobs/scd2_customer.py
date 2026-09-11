from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.spark import get_spark

spark = get_spark(
    "sentinel-scd2-customer"
)

src = (
    spark.read
    .format("delta")
    .load("data/silver/customers")
    .select(
        "customer_id",
        "customer_name",
        "email",
        "region",
        "date_of_birth",
    )
)

tracked_attributes = [
    "customer_name",
    "email",
    "region",
    "date_of_birth",
]

src = src.withColumn(
    "_hash",
    F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(column).cast("string"),
                    F.lit(""),
                )
                for column in tracked_attributes
            ],
        ),
        256,
    ),
)

path = "data/gold/dim_customer_scd2"

run_timestamp = datetime.utcnow()


if not DeltaTable.isDeltaTable(
    spark,
    path,
):

    (
        src.withColumn(
            "effective_from",
            F.lit(run_timestamp).cast(
                "timestamp"
            ),
        )
        .withColumn(
            "effective_to",
            F.lit(None).cast(
                "timestamp"
            ),
        )
        .withColumn(
            "is_current",
            F.lit(True),
        )
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)
    )

else:

    target = DeltaTable.forPath(
        spark,
        path,
    )

    current = (
        target.toDF()
        .filter(
            F.col("is_current") == True
        )
    )

    changed = (
        src.alias("s")
        .join(
            current.alias("t"),
            "customer_id",
            "left",
        )
        .filter(
            F.col(
                "t.customer_id"
            ).isNull()
            |
            (
                F.col("s._hash")
                != F.col("t._hash")
            )
        )
        .select("s.*")
    )

    changed_count = changed.count()

    if changed_count > 0:

        changed_ids = (
            changed
            .select("customer_id")
            .distinct()
        )

        (
            target.alias("t")
            .merge(
                changed_ids.alias("s"),
                (
                    "t.customer_id = "
                    "s.customer_id "
                    "AND t.is_current = true"
                ),
            )
            .whenMatchedUpdate(
                set={
                    "is_current": "false",
                    "effective_to": (
                        "CAST("
                        f"'{run_timestamp}' "
                        "AS TIMESTAMP)"
                    ),
                }
            )
            .execute()
        )

        (
            changed
            .withColumn(
                "effective_from",
                F.lit(run_timestamp).cast(
                    "timestamp"
                ),
            )
            .withColumn(
                "effective_to",
                F.lit(None).cast(
                    "timestamp"
                ),
            )
            .withColumn(
                "is_current",
                F.lit(True),
            )
            .write
            .format("delta")
            .mode("append")
            .save(path)
        )