# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    获取配置
"""
import toml

def get_config(file_path,section=None):
    """
    获取配置
    :param file_path: 配置文件地址
    :param section: 关键字
    :return: 配置结果，dict
    """
    cfg=toml.load(file_path)
    if section==None:
        return cfg
    return cfg[section]