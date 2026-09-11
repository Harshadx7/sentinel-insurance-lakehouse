from src.common.config import load_entities
def test_entities_config_loads():
    e=load_entities(); assert "customers" in e; assert e["customers"]["key"]=="customer_id"
