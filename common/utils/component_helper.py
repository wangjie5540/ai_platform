# coding: utf-8

# 组件相关的工具类

import common.constants.global_constant as global_constant
import os


def pass_output(output_params: dict):
    """
    需要传递给下游组件的参数集合
    :param output_params:
    :return:
    """
    os.makedirs(global_constant.component_output_path, exist_ok=True)
    if output_params:
        for key, value in output_params.items():
            save_path = os.path.join(global_constant.component_output_path, key)
            print(save_path)
            with open(save_path, 'w') as f:
                f.write(value)


def get_output(component_name):
    return os.path.join(global_constant.component_output_path, component_name)
