from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import Window, functions as F

from src.common.audit import write_audit
from src.common.config import load_entities
from src.common.data_quality import (
    non_negative,
    required,
    status,
)
from src.common.spark import get_spark

spark = get_spark("sentinel-silver-merge")

run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")


def latest_by_key(df, key):
    """
    Keep the latest version of each business key.
    updated_at is preferred when available, followed by ingestion time.
    """

    if "updated_at" in df.columns:

        df = df.withColumn(
            "updated_at",
            F.to_timestamp("updated_at"),
        )

        ordering = [
            F.col("updated_at").desc(),
            F.col("_ingested_at").desc(),
        ]

    else:

        ordering = [
            F.col("_ingested_at").desc(),
        ]

    window = (
        Window
        .partitionBy(key)
        .orderBy(*ordering)
    )

    return (
        df.withColumn(
            "_rn",
            F.row_number().over(window),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


for entity, cfg in load_entities().items():

    key = cfg["key"]

    try:

        bronze = (
            spark.read
            .format("delta")
            .load(f"data/bronze/{entity}")
        )

        latest = latest_by_key(
            bronze,
            key,
        )

        checked = status(
            non_negative(
                required(
                    latest,
                    cfg.get(
                        "required_columns",
                        [],
                    ),
                ),
                cfg.get(
                    "amount_columns",
                    [],
                ),
            )
        )

        valid = checked.filter(
            F.col("dq_status") == "VALID"
        )

        invalid = checked.filter(
            F.col("dq_status") == "QUARANTINE"
        )

        source_count = latest.count()
        valid_count = valid.count()
        invalid_count = invalid.count()

        silver_path = f"data/silver/{entity}"

        if DeltaTable.isDeltaTable(
            spark,
            silver_path,
        ):

            target = DeltaTable.forPath(
                spark,
                silver_path,
            )

            (
                target.alias("t")
                .merge(
                    valid.alias("s"),
                    f"t.{key} = s.{key}",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        else:

            (
                valid.write
                .format("delta")
                .mode("overwrite")
                .save(silver_path)
            )

        (
            invalid.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true",
            )
            .save(
                f"data/quarantine/{entity}"
            )
        )

        write_audit(
            spark=spark,
            run_id=run_id,
            job_name="silver_transform",
            entity=entity,
            status="SUCCESS",
            source_count=source_count,
            target_count=valid_count,
            quarantine_count=invalid_count,
        )

    except Exception as exc:

        write_audit(
            spark=spark,
            run_id=run_id,
            job_name="silver_transform",
            entity=entity,
            status="FAILED",
            message=str(exc),
        )

        raise