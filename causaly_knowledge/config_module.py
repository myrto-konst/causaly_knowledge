import yaml
import os

config = {}

def _load_config(file_str: str) -> None:
    global config
    with open(file_str, "r") as f:
        config.update(yaml.load(f, Loader=yaml.FullLoader))
        config['server_credentials'] ={k: os.environ[v] for k, v in  config['server_credentials'].items()}

def get_config(file_str: str) -> dict:
    _load_config(file_str=file_str)
    
    return config