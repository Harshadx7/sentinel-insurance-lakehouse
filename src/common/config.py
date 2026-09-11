from pathlib import Path
import yaml
def load_entities(config_path="config/entities.yml"):
    path=Path(config_path)
    if not path.exists(): raise FileNotFoundError(f"Entity configuration not found: {path}")
    with path.open(encoding="utf-8") as f: data=yaml.safe_load(f) or {}
    entities=data.get("entities",{})
    if not entities: raise ValueError("No entities found in configuration")
    return entities
