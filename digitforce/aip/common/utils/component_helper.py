# coding: utf-8
import json
import os

import digitforce.aip.common.constants.global_constant as global_constant


def write_output(name: str, parameters, need_json_dump: bool = False):
    """
    向下游组件写入参数
    :param name:
    :param parameters:
    :param need_json_dump: 内容是否需要json序列化
    :return:
    """
    content = parameters
    if need_json_dump and isinstance(parameters, dict):
        content = json.dumps(parameters, ensure_ascii=False)
    with open(generate_output_path(name), 'w') as f:
        f.write(content)


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


def generate_output_path(name: str):
    """
    生成输出文件路径
    """
    return f'/tmp/{name}'
