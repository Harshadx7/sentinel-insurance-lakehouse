from pyspark.sql import DataFrame, functions as F
def required(df: DataFrame, columns: list[str]) -> DataFrame:
    if not columns: return df.withColumn("_dq_required_failed",F.lit(False))
    failure=None
    for c in columns:
        rule=F.col(c).isNull() | (F.trim(F.col(c).cast("string"))=="")
        failure=rule if failure is None else failure | rule
    return df.withColumn("_dq_required_failed",failure)
def non_negative(df: DataFrame, columns: list[str]) -> DataFrame:
    if not columns: return df.withColumn("_dq_negative_failed",F.lit(False))
    failure=None
    for c in columns:
        rule=F.col(c)<0
        failure=rule if failure is None else failure | rule
    return df.withColumn("_dq_negative_failed",failure)
def status(df: DataFrame) -> DataFrame:
    bad=F.coalesce(F.col("_dq_required_failed"),F.lit(False)) | F.coalesce(F.col("_dq_negative_failed"),F.lit(False))
    return df.withColumn("dq_status",F.when(bad,"QUARANTINE").otherwise("VALID"))
