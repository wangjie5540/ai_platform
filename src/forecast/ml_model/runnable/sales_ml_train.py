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

from pyspark.sql import SparkSession

def spark_init():
    """
    初始化特征
    :return:
    """
    os.environ["PYSPARK_DRIVER_PYTHON"]="/data/ibs/anaconda3/bin/python"
    os.environ['PYSPARK_PYTHON']="/data/ibs/anaconda3/bin/python"
    spark=SparkSession.builder \
        .appName("gxc_test").master('yarn') \
        .config("spark.executor.instances", "8") \
        .config("spark.executor.memory", "16g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.maxResultSize", "6g") \
        .config("spark.default.parallelism", "600") \
        .config("spark.network.timeout", "240s") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.join.enabled", "true") \
        .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128000000") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "1") \
        .config("spark.dynamicAllocation.maxExecutors", "10")\
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.dynamicAllocation.enabled","false")\
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamici.partition", True) \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("hive.exec.max.dynamic.partitions", "10000") \
        .enableHiveSupport().getOrCreate()
    spark.sql("set hive.exec.dynamic.partitions=true")
    spark.sql("set hive.exec.max.dynamic.partitions=2048")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("use ai_dm_dev")
    sc = spark.sparkContext
    zip_path = './forecast.zip'
    zip_path_1 = './digitforce.zip'
    sc.addPyFile(zip_path)
    sc.addPyFile(zip_path_1)
    return spark

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
        'step_len': 1,
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


def run(spark):
    """
    跑接口
    :return:
    """

    param = param_default()
    if isinstance(param, str):
        param = json.loads(param)
    ml_model_train(param, spark)


if __name__ == "__main__":
    spark = spark_init()
    run(spark)
#     spark.stop()
