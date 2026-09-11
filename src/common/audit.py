from datetime import datetime
AUDIT_PATH="data/control/pipeline_audit"
def write_audit(spark,run_id,job_name,entity,status,source_count=None,target_count=None,quarantine_count=None,message=None):
    df=spark.createDataFrame([(run_id,job_name,entity,status,source_count,target_count,quarantine_count,message,datetime.utcnow())],
        ["run_id","job_name","entity","status","source_count","target_count","quarantine_count","message","event_ts"])
    df.write.format("delta").mode("append").save(AUDIT_PATH)
