# coding: utf-8

import digitforce.aip.common.constants.global_constant as global_constant
import json


def write_output(parameters: dict):
    """
    向下游组件写入参数
    """
    with open(global_constant.JSON_OUTPUT_PATH, 'w') as f:
        f.write(json.dumps(parameters))


def read_input(input_path) -> dict:
    """
    读取上游组件入参
    :return:
    """
    with open(input_path, 'r') as f:
        return json.loads(f.read())
