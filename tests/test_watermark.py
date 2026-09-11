from datetime import datetime

from src.common.watermark import (
    apply_watermark,
)


def test_watermark_filters_only_newer_records(
    spark,
):

    df = spark.createDataFrame(
        [
            (
                "A",
                datetime(
                    2026,
                    1,
                    1,
                ),
            ),
            (
                "B",
                datetime(
                    2026,
                    1,
                    2,
                ),
            ),
            (
                "C",
                datetime(
                    2026,
                    1,
                    3,
                ),
            ),
        ],
        [
            "id",
            "updated_at",
        ],
    )

    result = apply_watermark(
        df=df,
        watermark_column="updated_at",
        previous_watermark=datetime(
            2026,
            1,
            2,
        ),
    )

    ids = [
        row["id"]
        for row in result.collect()
    ]

    assert ids == ["C"]


def test_no_watermark_returns_all_records(
    spark,
):

    df = spark.createDataFrame(
        [
            (
                "A",
                datetime(
                    2026,
                    1,
                    1,
                ),
            ),
            (
                "B",
                datetime(
                    2026,
                    1,
                    2,
                ),
            ),
        ],
        [
            "id",
            "updated_at",
        ],
    )

    result = apply_watermark(
        df=df,
        watermark_column="updated_at",
        previous_watermark=None,
    )

    assert result.count() == 2