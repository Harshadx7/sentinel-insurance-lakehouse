SELECT *,
CASE WHEN fraud_score>=75 THEN 'HIGH'
     WHEN fraud_score>=40 THEN 'MEDIUM'
     ELSE 'LOW' END AS fraud_risk_band
FROM {{ ref('stg_claims') }}
