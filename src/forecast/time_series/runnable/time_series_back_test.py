# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：对外提供的接口：回测
"""
import argparse
import logging
import os

try:
    import findspark  # 使用spark-submit 的cluster时要注释掉

    findspark.init()
except:
    pass
import sys
import json
import traceback

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(file_path)

from forecast.time_series.sp.backup_test_for_time_series_sp import back_test_sp
# from forecast.common.log import get_logger
# from forecast.common.config import get_config
# from forecast.common.data_helper import update_param_default

from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config
from digitforce.aip.common.data_helper import update_param_default

from pyspark.sql import SparkSession


def spark_init():
    """
    初始化特征
    :return:
    """
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/data/ibs/anaconda3/bin/python"
    os.environ['PYSPARK_PYTHON'] = "/data/ibs/anaconda3/bin/python"
    spark = SparkSession.builder \
        .appName("model_test").master('yarn') \
        .config("spark.executor.instances", "50") \
        .config("spark.executor.memory", "4g") \
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
        .config("spark.shuffle.service.enabled", "true") \
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
    sc.addPyFile(zip_path)
    return spark


def time_series_back_test(param, spark=None):
    """
    预测模型回测
    :param param: 所需参数
    :param spark: spark，如果不传入则会内部启动一个运行完关闭
    :return:成功：True 失败：False
    """

    logger_info = setup_console_log(level=logging.INFO)
    setup_logging(info_log_file="time_series_back_test.info", error_log_file="", info_log_file_level="INFO")
    logging.info("==============================LOADING============================")
    mode_type = 'sp'  # 先给个默认值
    status = False
    if 'mode_type' in param.keys():
        mode_type = param['mode_type']
    try:
        if mode_type == 'sp':  # spark版本
            logging.info("RUNNING......")
            status = back_test_sp(param, spark)
        else:  # pandas版本
            pass

        logging.info("SUCCESS")
    except Exception as e:
        logging.info(traceback.format_exc())
        status = "FAIL"
    return status


def get_default_conf():
    """
    获取时序预测所需的默认参数
    :return: 默认参数
    """
    file_tmp = "forecast/time_series/config/"
    time_series_operation = file_tmp + 'operation.toml'
    time_series = file_tmp + r'model.toml'
    sales_data_file = file_tmp + 'sales_data.toml'
    conf_default = get_config(time_series_operation, None)
    method_param_all = get_config(time_series, None)  # 模型参数
    conf_default['method_param_all'] = method_param_all
    sales_data_dict = get_config(sales_data_file, 'data')
    conf_default.update(sales_data_dict)
    return conf_default


def run(forecast_start_date, purpose, time_type):
    """
    跑接口
    :return:
    """
    param = {"forecast_start_date": forecast_start_date, "purpose": purpose, "time_type": time_type}
    default_conf = get_default_conf()
    if isinstance(param, str):
        param = json.loads(param)
    param = update_param_default(param, default_conf)
    spark = spark_init()
    time_series_back_test(param, spark)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--forecast_start_date', type=str, default=None, help='input forecast_start_date')
    parser.add_argument('-p', '--purpose', type=str, default='back_test', help='input purpose')
    parser.add_argument('-t', '--time_type', type=str, default='day', help='input time_type')
    args = parser.parse_args()
    run(args.forecast_start_date, args.purpose, args.time_type)


if __name__ == "__main__":
    main()
