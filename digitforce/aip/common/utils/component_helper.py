# coding: utf-8
import os

import digitforce.aip.common.constants.global_constant as global_constant
import json


def write_output(parameters: dict):
    """
    向下游组件写入参数
    """
    with open(global_constant.JSON_OUTPUT_PATH, 'w') as f:
        f.write(json.dumps(parameters))


def read_input(input_path=global_constant.JSON_OUTPUT_PATH) -> dict:
    """
    读取上游组件入参
    :return:
    """
    with open(input_path, 'r') as f:
        return json.loads(f.read())


def set_component_app_name(app_name):
    """
    设置组件名的环境变量
    """
    os.environ[global_constant.SPARK_APP_NAME] = app_name


def get_component_app_name():
    """
    获取app_name
    """
    return os.getenv(global_constant.SPARK_APP_NAME, 'default_app_name')
