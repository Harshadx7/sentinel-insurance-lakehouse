from src.common.data_quality import (
    non_negative,
    required,
    status,
)


def test_required_validation_flags_null_and_blank(
    spark,
):

    df = spark.createDataFrame(
        [
            ("A",),
            (None,),
            ("",),
            ("   ",),
        ],
        ["value"],
    )

    result = (
        status(
            required(
                df,
                ["value"],
            )
        )
        .select(
            "value",
            "dq_status",
        )
        .collect()
    )

    statuses = [
        row["dq_status"]
        for row in result
    ]

    assert statuses == [
        "VALID",
        "QUARANTINE",
        "QUARANTINE",
        "QUARANTINE",
    ]


def test_non_negative_validation_flags_negative_values(
    spark,
):

    df = spark.createDataFrame(
        [
            (100.0,),
            (0.0,),
            (-25.0,),
        ],
        ["amount"],
    )

    result = (
        status(
            non_negative(
                df,
                ["amount"],
            )
        )
        .select(
            "amount",
            "dq_status",
        )
        .collect()
    )

    statuses = [
        row["dq_status"]
        for row in result
    ]

    assert statuses == [
        "VALID",
        "VALID",
        "QUARANTINE",
    ]