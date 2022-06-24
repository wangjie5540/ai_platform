# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
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
from common.log import get_logger


#为了开发测试用，正式环境记得删除
def param_default():
    param = {
        'mode_type': 'sp',
        'sdate': '',
        'edate': ''
    }
    return param

def parse_arguments():
    """
    解析参数
    :return:
    """
    param = param_default()#开发测试用
    parser = argparse.ArgumentParser(description='time series predict')
    parser.add_argument('--param', default=param, help='arguments')
    parser.add_argument('--spark', default=None, help='spark')
    args = parser.parse_args()
    return args


def run():
    """
    跑接口
    :return:
    """
    logger_info = get_logger()
    logger_info.info("LOADING···")
    args = parse_arguments()
    param = args.param
    spark = args.spark
    if isinstance(param, str):
        param = json.loads(param)
    logger_info.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logger_info.info("RUNNING···")
            # train_sp(param, spark)
        else:
            # pandas版本
            pass
        status = "SUCCESS"
        logger_info.info("SUCCESS")
    except Exception as e:
        status = "ERROR"
        logger_info.info(traceback.format_exc())
    return status

if __name__ == "__main__":
    run()