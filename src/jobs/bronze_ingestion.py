from datetime import datetime

from pyspark.sql import functions as F

from src.common.audit import write_audit
from src.common.config import load_entities
from src.common.spark import get_spark
from src.common.watermark import (
    apply_watermark,
    get_watermark,
    set_watermark,
)

spark = get_spark("sentinel-bronze-incremental")

run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")


for entity, cfg in load_entities().items():

    watermark_column = cfg["watermark_column"]

    try:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(f"data/raw/{entity}.csv")
            .withColumn(
                watermark_column,
                F.to_timestamp(watermark_column),
            )
        )

        previous_watermark = get_watermark(
            spark,
            entity,
        )

        df = apply_watermark(
            df,
            watermark_column,
            previous_watermark,
        )

        source_count = df.count()

        if source_count == 0:

            write_audit(
                spark=spark,
                run_id=run_id,
                job_name="bronze_ingestion",
                entity=entity,
                status="SUCCESS",
                source_count=0,
                target_count=0,
                message="No new records after watermark",
            )

            continue

        batch = (
            df.withColumn(
                "_source_file",
                F.input_file_name(),
            )
            .withColumn(
                "_ingested_at",
                F.current_timestamp(),
            )
            .withColumn(
                "_batch_id",
                F.lit(run_id),
            )
        )

        (
            batch.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(f"data/bronze/{entity}")
        )

        maximum_watermark = (
            batch.agg(
                F.max(watermark_column).alias("watermark")
            )
            .collect()[0]["watermark"]
        )

        set_watermark(
            spark=spark,
            entity=entity,
            value=maximum_watermark,
            batch_id=run_id,
        )

        write_audit(
            spark=spark,
            run_id=run_id,
            job_name="bronze_ingestion",
            entity=entity,
            status="SUCCESS",
            source_count=source_count,
            target_count=source_count,
            message=(
                f"Watermark advanced to "
                f"{maximum_watermark}"
            ),
        )

    except Exception as exc:

        write_audit(
            spark=spark,
            run_id=run_id,
            job_name="bronze_ingestion",
            entity=entity,
            status="FAILED",
            message=str(exc),
        )

        raise