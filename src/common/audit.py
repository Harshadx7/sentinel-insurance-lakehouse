from datetime import datetime

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

AUDIT_PATH = "data/control/pipeline_audit"

AUDIT_SCHEMA = StructType([
    StructField(
        "run_id",
        StringType(),
        False,
    ),
    StructField(
        "job_name",
        StringType(),
        False,
    ),
    StructField(
        "entity",
        StringType(),
        True,
    ),
    StructField(
        "status",
        StringType(),
        False,
    ),
    StructField(
        "source_count",
        LongType(),
        True,
    ),
    StructField(
        "target_count",
        LongType(),
        True,
    ),
    StructField(
        "quarantine_count",
        LongType(),
        True,
    ),
    StructField(
        "message",
        StringType(),
        True,
    ),
    StructField(
        "event_ts",
        TimestampType(),
        False,
    ),
])


def write_audit(
    spark,
    run_id,
    job_name,
    entity,
    status,
    source_count=None,
    target_count=None,
    quarantine_count=None,
    message=None,
):

    row = [
        (
            run_id,
            job_name,
            entity,
            status,
            source_count,
            target_count,
            quarantine_count,
            message,
            datetime.utcnow(),
        )
    ]

    df = spark.createDataFrame(
        row,
        schema=AUDIT_SCHEMA,
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .save(AUDIT_PATH)
    )