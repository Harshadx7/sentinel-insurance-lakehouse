from src.common.reconciliation import (
    reconcile_key_sets,
)


def test_reconciliation_passes_at_deduplicated_key_grain(
    spark,
):

    source = spark.createDataFrame(
        [
            ("C001",),
            ("C002",),
            ("C003",),
        ],
        ["customer_id"],
    )

    silver = spark.createDataFrame(
        [
            ("C001",),
            ("C002",),
        ],
        ["customer_id"],
    )

    quarantine = spark.createDataFrame(
        [
            ("C003",),
        ],
        ["customer_id"],
    )

    result = reconcile_key_sets(
        source_keys=source,
        silver_keys=silver,
        quarantine_keys=quarantine,
        key="customer_id",
    )

    assert result["status"] == "PASS"
    assert result["missing_count"] == 0
    assert result["unexpected_count"] == 0
    assert result["classified_count"] == 3


def test_reconciliation_fails_when_source_key_is_missing(
    spark,
):

    source = spark.createDataFrame(
        [
            ("C001",),
            ("C002",),
            ("C003",),
        ],
        ["customer_id"],
    )

    silver = spark.createDataFrame(
        [
            ("C001",),
        ],
        ["customer_id"],
    )

    quarantine = spark.createDataFrame(
        [
            ("C002",),
        ],
        ["customer_id"],
    )

    result = reconcile_key_sets(
        source_keys=source,
        silver_keys=silver,
        quarantine_keys=quarantine,
        key="customer_id",
    )

    assert result["status"] == "FAIL"
    assert result["missing_count"] == 1