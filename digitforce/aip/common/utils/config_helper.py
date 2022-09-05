# coding: utf-8

import yaml
import digitforce.aip.common.constants.global_constant as global_constant


def get_module_config(module_name):
    with open(global_constant.AIP_CONFIG_PATH, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        return config.get(module_name, {})
