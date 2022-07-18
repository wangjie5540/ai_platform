# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    获取日志
"""

import os
from logging.config import fileConfig
import logging
import configparser

def mkdir_floder_not_exist(filename):
    """
    如果文件夹不存在则创建
    :param filename: 文件夹地址
    :return:
    """
    if os.path.exists(filename) == False:
        os.makedirs(filename)

def get_logger():
    """
    获取日志
    :return: 
    """
    cf= configparser.ConfigParser()
    file_config="forecast/common/config/logging.ini"
    cf.read(file_config,encoding='utf-8-sig')

    file_path = cf['handler_fileHandler']['file_path']
    mkdir_floder_not_exist(file_path)
    fileConfig(file_config)
    file_logger=logging.getLogger('file')
    return file_logger