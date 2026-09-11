from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

WATERMARK_PATH = "data/control/watermarks"

WATERMARK_SCHEMA = StructType([
    StructField("entity", StringType(), False),
    StructField("watermark_value", TimestampType(), False),
    StructField("batch_id", StringType(), False),
    StructField("updated_at", TimestampType(), False),
])


def apply_watermark(df, watermark_column, previous_watermark):
    """
    Return all rows when no watermark exists.
    Otherwise return only rows newer than the last successful watermark.
    """
    if previous_watermark is None:
        return df

    return df.filter(
        F.col(watermark_column) >
        F.lit(previous_watermark).cast("timestamp")
    )


def get_watermark(spark, entity, path=WATERMARK_PATH):
    """
    Return the latest persisted watermark for an entity.
    Returns None only when the watermark Delta table does not yet exist
    or when the entity has no persisted watermark.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        return None

    rows = (
        spark.read
        .format("delta")
        .load(path)
        .filter(F.col("entity") == entity)
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )

    return rows[0]["watermark_value"] if rows else None


def set_watermark(
    spark,
    entity,
    value,
    batch_id,
    path=WATERMARK_PATH,
):
    """
    Persist the maximum successfully ingested watermark.
    """
    if value is None:
        return

    source = spark.createDataFrame(
        [
            (
                entity,
                value,
                batch_id,
                datetime.utcnow(),
            )
        ],
        schema=WATERMARK_SCHEMA,
    )

    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)

        (
            target.alias("t")
            .merge(
                source.alias("s"),
                "t.entity = s.entity",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            source.write
            .format("delta")
            .mode("overwrite")
            .save(path)
        )