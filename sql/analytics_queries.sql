-- Fraud triage: highest-risk claim per policy
WITH ranked AS (
 SELECT claim_id,policy_id,claim_status,incurred_amount,fraud_score,
 ROW_NUMBER() OVER(PARTITION BY policy_id ORDER BY fraud_score DESC,incurred_amount DESC) rn
 FROM fact_claim
)
SELECT * FROM ranked WHERE fraud_score>=75 AND rn=1;

-- Product loss ratio
SELECT p.product,
SUM(fp.written_premium) written_premium,
SUM(COALESCE(fc.incurred_amount,0)) incurred_claims,
ROUND(SUM(COALESCE(fc.incurred_amount,0))/NULLIF(SUM(fp.written_premium),0),4) loss_ratio
FROM fact_policy_premium fp
JOIN dim_policy p ON p.policy_id=fp.policy_id
LEFT JOIN fact_claim fc ON fc.policy_id=fp.policy_id
GROUP BY p.product ORDER BY loss_ratio DESC;
