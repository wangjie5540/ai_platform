# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    数据分组：对外提供的接口
"""
import os
try:
    import findspark #使用spark-submit 的cluster时要注释掉
    findspark.init()
except:
    pass
import sys
import json
import argparse
import traceback
file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(file_path)#解决不同位置调用依赖包路径问题
from common.log import get_logger
from data_split.sp.data_split_sp import data_split_sp

def data_split(param,spark=None):
    """
    数据分组
    :param param:参数
    :param spark: spark
    :return:
    """
    logger_info=get_logger()
    mode_type='sp'#先给个默认值
    status=False
    if 'mode_type' in param.keys():
        mode_type=param['mode_type']
    try:
        if mode_type=='sp':#spark版本
            status=data_split_sp(param,spark)
        else:#pandas版本
            pass
        logger_info.info(str(param))
    except Exception as e:
        logger_info.error(traceback.format_exc())
    return status

def parse_arguments():
    """
    解析参数
    :return:
    """
    parser=argparse.ArgumentParser(description='model selection and grouping')
    parser.add_argument('--param',default={},help='arguments')
    parser.add_argument('--spark',default=None,help='spark')
    args=parser.parse_args()
    return args

def run():
    """
    跑接口
    :return:
    """
    args=parse_arguments()
    param=args.param
    spark=args.spark
    if isinstance(param, str):
        param=json.loads(param)
    data_split(param,spark)

if __name__ == "__main__":
    run()