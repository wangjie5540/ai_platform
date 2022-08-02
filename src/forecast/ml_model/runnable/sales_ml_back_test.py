# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    机器学习模型：对外提供的接口
"""
import os

import sys
if 'ipykernel' in sys.modules:
    import findspark  # 使用spark-submit 的cluster时要注释掉

    findspark.init()
else:
    pass
import json
import argparse
import traceback
from forecast.ml_model.sp.back_test_sp import back_test_sp
# from digitforce.aip.common.spark_helper import SparkHelper,forecast_spark_session
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
import logging
from digitforce.aip.common.spark_init import forecast_spark_session
import zipfile

logger_info = setup_console_log()
setup_logging(info_log_file="sales_fill_zero.info", error_log_file="", info_log_file_level="INFO")

file_path = os.path.abspath(os.path.join(os.getcwd(), '../../'))
sys.path.append(file_path)


def ml_model_back_test(param, spark=None):
    """
    #机器学习模型预测
    :param param: 所需参数
    :param spark: spark，如果不传入则会内部启动一个运行完关闭
    :return:成功：True 失败：False
    """

    mode_type = 'sp'  # 先给个默认值
    status = False
    if 'mode_type' in param.keys():
        mode_type = param['mode_type']

    if mode_type == 'sp':  # spark版本
        status = back_test_sp(param, spark)
    else:  # pandas版本
        pass
    logging.info(str(param))

    return status


def param_default():
    param = {
        'ts_model_list': ['lightgbm'],
        'y_type_list': ['c'],
        'mode_type': 'sp',
        'forcast_start_date': '20211009',
        'bt_sdate': '20211001',
        'predict_len': 14,
        'col_keys': ['shop_id', 'group_category', 'apply_model'],
        'apply_model_index': 2,
        'step_len': 1,
        'purpose': 'back_test'
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


def run(spark_,sdate, edate):
    """

    :param spark_:
    :param edat:
    :return:
    """
    #     args = parse_arguments()
    param = param_default()
    param['edate'] = edate
    param['sdate'] = sdate
    #     param = args.param
    #     spark = args.spark
    if isinstance(param, str):
        param = json.loads(param)
    ml_model_back_test(param, spark_)


if __name__ == "__main__":
    files1 = zipfile.ZipFile('./forecast.zip', 'r')
    files2 = zipfile.ZipFile('./digitforce.zip', 'r')
    files1.extractall(os.getcwd())
    files2.extractall(os.getcwd())
    spark = forecast_spark_session("gaoxc_ml_back_test")
    if 'ipykernel' in sys.modules:
        sdate = '20201009'
        edate = '20211009'
    else:
        sdate, edate = sys.argv[1].replace('-', ''), sys.argv[2].replace('-', '')
    run(spark,sdate, edate)
