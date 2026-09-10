SELECT
 claim_id, policy_id,
 CAST(reported_date AS DATE) AS reported_date,
 UPPER(claim_status) AS claim_status,
 CAST(incurred_amount AS NUMBER(18,2)) AS incurred_amount,
 CAST(fraud_score AS NUMBER(5,2)) AS fraud_score
FROM {{ source('raw','claims') }}
WHERE claim_id IS NOT NULL
