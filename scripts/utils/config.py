import yaml
from scripts.utils import paths

def load_config(filename: str) -> dict:
    """
    Load YAML config file from configs directory.
    """
    config_path = paths.CONFIGS_DIR / filename
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError(f"Invalid config format in {filename}")

    return config
