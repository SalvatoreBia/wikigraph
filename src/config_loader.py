import json
from pathlib import Path

def load_config():
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "config.json"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)
