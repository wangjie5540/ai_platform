# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    机器学习模型：对外提供的接口
"""
import os

try:
    import findspark  # 使用spark-submit 的cluster时要注释掉

    findspark.init()
except:
    pass
import sys
import json
import argparse
import traceback
from forecast.ml_model.sp.train_sp import train_sp
# from digitforce.aip.common.spark_helper import SparkHelper,forecast_spark_session
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
import logging
logger_info = setup_console_log()
setup_logging(info_log_file="sales_fill_zero.info", error_log_file="", info_log_file_level="INFO")

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(file_path)  # 解决不同位置调用依赖包路径问题


def ml_model_train(param, spark=None):
    """
    #机器学习模型预测
    :param param: 所需参数
    :param spark: spark，如果不传入则会内部启动一个运行完关闭
    :return:成功：True 失败：False
    """

    status = False
    mode_type = 'sp'
    if 'mode_type' in param.keys():
        mode_type = param['mode_type']
    try:
        if mode_type == 'sp':  # spark版本
            status = train_sp(param, spark)
        else:  # pandas版本
            pass
        logging.info(str(param))
    except Exception as e:
        logging.info(traceback.format_exc())
    return status


# 为了开发测试用，正式环境记得删除
def param_default():
    param = {
        'ts_model_list': ['lightgbm'],
        'y_type_list': ['c'],
        'mode_type': 'sp',
        'forcast_start_date': '20211009',
        'predict_len': 14,
        'col_keys': ['shop_id', 'group_category', 'apply_model'],
        'apply_model_index': 2,
        'step_len': 5,
        'purpose': 'train'
    }
    return param


def parse_arguments():
    """
    解析参数
    :return:
    """
    param = param_default()  # 开发测试用
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
    args = parse_arguments()
    param = args.param
    spark = args.spark
    if isinstance(param, str):
        param = json.loads(param)
    ml_model_train(param, spark)


if __name__ == "__main__":
    run()
