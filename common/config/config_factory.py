import json
import logging
import os


class ConfigFactory:
    def __init__(self, ):
        self.k_v = {}

    def read_config(self, config_path):
        if not os.path.exists(config_path) or config_path is None:
            logging.warning(f"the config file is not exists...{config_path}")
            return
        with open(config_path) as fi:
            if config_path.endswith(".json"):
                content = fi.read()
                json_obj = json.loads(content)
                self.k_v = json_obj
            else:
                for _ in fi:
                    line = _.strip()
                    if line.startswith("#") or line.startswith("//"):
                        continue
                    tmp = line.find("=")
                    k = line[:tmp].strip()
                    v = line[tmp + 1:].strip()
                    self.k_v[k] = v

    def get_config_value(self, key):
        return self.k_v[key]


def is_test_model():
    return os.environ.get("RUN_MODEL", "PRODUCT").lower().strip() == "test"


dg_config_factory = ConfigFactory()
for config_file in ["/data/.digitforce_ai_platform/common_config", ".digitforce_ai_platform/common_config"]:
    dg_config_factory.read_config(config_file)
