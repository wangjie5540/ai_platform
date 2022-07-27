# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    获取配置
"""
import toml
import os


def list_all_dict(dict_a):
    if isinstance(dict_a,dict) : #使用isinstance检测数据类型
        for x in range(len(dict_a)):
            temp_key = list(dict_a.keys())[x]
            temp_value = dict_a[temp_key]
            if isinstance(temp_value, str) and "f'{" in temp_value:
                dict_a[temp_key] = eval(temp_value)
            if isinstance(temp_value,dict):
                list_all_dict(temp_value)
    return dict_a


def get_config(file_path, section=None):
    """
    获取配置
    :param file_path: 配置文件地址
    :param section: 关键字
    :return: 配置结果，dict
    """
    exec_list = toml.load(os.getcwd()+"/forecast/common/config/environment.toml")['exec']['exec_list']
    for exec_item in exec_list:
        exec(exec_item, globals())
    cfg = toml.load(file_path)
    cfg = list_all_dict(cfg)
    if section==None:
        return cfg
    return cfg[section]


def get_default_conf(file_list,section_list):
    param = {}
    if len(file_list)>0 and len(section_list)>0:
        for file_section in zip(file_list,section_list):
            file_toml = file_section[0]
            section = file_section[1]
            cfg = get_config(file_toml,section)
            param.update(cfg)
    else:
        return None
    return param


