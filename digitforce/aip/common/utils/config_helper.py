# coding: utf-8

import yaml
import os
import digitforce.aip.common.constants.global_constant as global_constant


def get_module_config(module_name):
    with open(global_constant.AIP_CONFIG_PATH, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        return config.get(module_name, {})


def get_component_config(module_name):
    if not os.path.exists(global_constant.COMPONENT_CONFIG_PATH):
        return {}
    with open(global_constant.COMPONENT_CONFIG_PATH, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        return config.get(module_name, {})
