from pyspark.sql import DataFrame, functions as F


def required(
    df: DataFrame,
    columns: list[str],
) -> DataFrame:

    failure = None

    for c in columns:

        rule = (
            F.col(c).isNull()
            | (
                F.trim(
                    F.col(c).cast("string")
                )
                == ""
            )
        )

        failure = (
            rule
            if failure is None
            else failure | rule
        )

    if failure is None:
        failure = F.lit(False)

    return df.withColumn(
        "_dq_required_failed",
        failure,
    )


def non_negative(
    df: DataFrame,
    columns: list[str],
) -> DataFrame:

    failure = None

    for c in columns:

        rule = F.col(c) < 0

        failure = (
            rule
            if failure is None
            else failure | rule
        )

    if failure is None:
        failure = F.lit(False)

    return df.withColumn(
        "_dq_negative_failed",
        failure,
    )


def status(
    df: DataFrame,
) -> DataFrame:

    required_failed = (
        F.coalesce(
            F.col("_dq_required_failed"),
            F.lit(False),
        )
        if "_dq_required_failed" in df.columns
        else F.lit(False)
    )

    negative_failed = (
        F.coalesce(
            F.col("_dq_negative_failed"),
            F.lit(False),
        )
        if "_dq_negative_failed" in df.columns
        else F.lit(False)
    )

    bad = (
        required_failed
        | negative_failed
    )

    return df.withColumn(
        "dq_status",
        F.when(
            bad,
            "QUARANTINE",
        ).otherwise(
            "VALID"
        ),
    )