from __future__ import annotations
import os, random, uuid
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42); random.seed(42)
OUT = Path(os.getenv("DATA_DIR", "data/raw"))
N_CUSTOMERS = int(os.getenv("N_CUSTOMERS", "5000"))
N_POLICIES = int(os.getenv("N_POLICIES", "8000"))
N_CLAIMS = int(os.getenv("N_CLAIMS", "2500"))

REGIONS=["APAC","EMEA","AMERICAS"]
PRODUCTS=["MOTOR","HEALTH","PROPERTY","TRAVEL"]
POLICY_STATUS=["ACTIVE","LAPSED","CANCELLED","EXPIRED"]
CLAIM_STATUS=["OPEN","INVESTIGATING","APPROVED","PAID","REJECTED"]

def save(name, rows):
    OUT.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT/f"{name}.csv", index=False)

def main():
    now=datetime.utcnow()
    customers=[]
    for i in range(1,N_CUSTOMERS+1):
        customers.append({"customer_id":f"C{i:08d}","customer_name":fake.name(),
        "email":fake.email(),"date_of_birth":fake.date_of_birth(18,80).isoformat(),
        "region":random.choice(REGIONS),"updated_at":(now-timedelta(days=random.randint(0,365))).isoformat()})
    brokers=[{"broker_id":f"B{i:05d}","broker_name":fake.company(),
              "region":random.choice(REGIONS),"updated_at":now.isoformat()} for i in range(1,301)]
    policies=[]
    for i in range(1,N_POLICIES+1):
        start=now-timedelta(days=random.randint(1,1095))
        policies.append({"policy_id":f"P{i:09d}","customer_id":random.choice(customers)["customer_id"],
        "broker_id":random.choice(brokers)["broker_id"],"product":random.choice(PRODUCTS),
        "policy_status":random.choices(POLICY_STATUS,[70,10,5,15])[0],
        "effective_date":start.date().isoformat(),"expiry_date":(start+timedelta(days=365)).date().isoformat(),
        "written_premium":round(random.uniform(250,12000),2),"currency":random.choice(["AUD","USD","GBP"]),
        "updated_at":(now-timedelta(days=random.randint(0,90))).isoformat()})
    claims=[]; payments=[]
    for i in range(1,N_CLAIMS+1):
        p=random.choice(policies); reported=now-timedelta(days=random.randint(1,730))
        incurred=round(random.uniform(100,max(1000,p["written_premium"]*8)),2)
        paid=round(incurred*random.uniform(0,1),2)
        cid=f"CL{i:010d}"
        claims.append({"claim_id":cid,"policy_id":p["policy_id"],"reported_date":reported.date().isoformat(),
        "claim_status":random.choice(CLAIM_STATUS),"claim_type":p["product"],"incurred_amount":incurred,
        "fraud_score":round(random.betavariate(1.2,5)*100,2),"updated_at":now.isoformat()})
        if paid > 0:
    payments.append({
        "payment_id": str(uuid.uuid4()),
        "claim_id": cid,
        "payment_date": (
            reported + timedelta(days=random.randint(1, 90))
        ).date().isoformat(),
        "payment_amount": paid,
        "payment_type": "INDEMNITY",
        "updated_at": now.isoformat()
    })
    for name,rows in [("customers",customers),("brokers",brokers),("policies",policies),("claims",claims),("payments",payments)]: save(name,rows)
    print(f"Generated {len(customers)} customers, {len(policies)} policies, {len(claims)} claims, {len(payments)} payments")
if __name__=="__main__": main()
