from pyspark.sql import Window
from pyspark.sql import functions as F


def latest_by_key(df, key):
    """
    Deduplicate Bronze to the same business-key grain
    used by the Silver transformation.
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


def reconcile_key_sets(
    source_keys,
    silver_keys,
    quarantine_keys,
    key,
):
    """
    Reconcile datasets at business-key grain.

    A source key is reconciled when it exists in either:
    - Silver
    - Quarantine

    The Silver/Quarantine overlap is retained as a diagnostic metric.
    """

    source = (
        source_keys
        .select(key)
        .distinct()
    )

    silver = (
        silver_keys
        .select(key)
        .distinct()
    )

    quarantine = (
        quarantine_keys
        .select(key)
        .distinct()
    )

    classified = (
        silver
        .unionByName(quarantine)
        .distinct()
    )

    missing = (
        source
        .join(
            classified,
            key,
            "left_anti",
        )
        .count()
    )

    unexpected = (
        classified
        .join(
            source,
            key,
            "left_anti",
        )
        .count()
    )

    overlap = (
        silver
        .join(
            quarantine,
            key,
            "inner",
        )
        .count()
    )

    source_count = source.count()
    silver_count = silver.count()
    quarantine_count = quarantine.count()
    classified_count = classified.count()

    status = (
        "PASS"
        if (
            missing == 0
            and unexpected == 0
            and source_count == classified_count
        )
        else "FAIL"
    )

    return {
        "source_count": source_count,
        "silver_count": silver_count,
        "quarantine_count": quarantine_count,
        "classified_count": classified_count,
        "missing_count": missing,
        "unexpected_count": unexpected,
        "overlap_count": overlap,
        "status": status,
    }


def reconcile_counts(
    spark,
    entity,
    key,
):
    """
    Read Bronze/Silver/Quarantine and reconcile
    at deduplicated business-key grain.
    """

    bronze = (
        spark.read
        .format("delta")
        .load(f"data/bronze/{entity}")
    )

    silver = (
        spark.read
        .format("delta")
        .load(f"data/silver/{entity}")
    )

    quarantine = (
        spark.read
        .format("delta")
        .load(
            f"data/quarantine/{entity}"
        )
    )

    latest_source = latest_by_key(
        bronze,
        key,
    )

    result = reconcile_key_sets(
        source_keys=latest_source,
        silver_keys=silver,
        quarantine_keys=quarantine,
        key=key,
    )

    result["entity"] = entity

    return result