# coding: utf-8

# 组件相关的工具类

import common.constants.global_constant as global_constant
import os


def pass_output(parameters: str, position: int):
    """
    输出的位置，从1开始，最终以out_x作为输出参数
    :param position: 输出的位置
    :return:
    """
    os.makedirs(global_constant.component_output_path, exist_ok=True)
    output_between_containers = os.path.join(global_constant.component_output_path, f'out_{position}')
    if parameters:
        with open(output_between_containers, 'w') as f:
            f.write(parameters)


def get_output(component_name):
    return os.path.join(global_constant.component_output_path, component_name)
