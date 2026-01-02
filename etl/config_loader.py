import json
import os


def load_config(config_file="config.json"):
    
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", config_file))
    with open(config_path, "r") as file:
        config = json.load(file)
    return config

