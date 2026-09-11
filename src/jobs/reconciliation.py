from datetime import datetime

from src.common.config import load_entities
from src.common.reconciliation import (
    reconcile_counts,
)
from src.common.spark import get_spark

spark = get_spark(
    "sentinel-reconciliation"
)

run_id = datetime.utcnow().strftime(
    "%Y%m%d%H%M%S"
)

rows = []


for entity, cfg in load_entities().items():

    key = cfg["key"]

    result = reconcile_counts(
        spark=spark,
        entity=entity,
        key=key,
    )

    result["run_id"] = run_id
    result["run_timestamp"] = (
        datetime.utcnow()
    )

    rows.append(result)


(
    spark.createDataFrame(rows)
    .write
    .format("delta")
    .mode("append")
    .save(
        "data/control/"
        "reconciliation_results"
    )
)