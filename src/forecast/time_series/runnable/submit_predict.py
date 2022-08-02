# -*- coding: utf-8 -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
time_series_predict
"""
import sys
import os

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(file_path)
from pyspark.sql import SparkSession
from forecast.time_series.sp.predict_for_time_series_sp import predict_sp
import logging
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config
import traceback
import zipfile


def forecast_spark_session(app_name):
    """
    初始化特征
    :return:
    """

    os.environ["PYSPARK_DRIVER_PYTHON"] = "/data/ibs/anaconda3/bin/python"
    os.environ['PYSPARK_PYTHON'] = "/data/ibs/anaconda3/bin/python"
    spark = SparkSession.builder \
        .appName(app_name).master('yarn') \
        .config("spark.yarn.queue", "shusBI") \
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
    zip_path1 = './forecast.zip'
    zip_path2 = './digitforce.zip'
    sc.addPyFile(zip_path1)
    sc.addPyFile(zip_path2)
    return spark


def load_params(forecast_start_date, purpose, time_type):
    param_cur = {"forecast_start_date": forecast_start_date,
                 "purpose": purpose,  # 该参数判断是预测还是回测，存在的意义是更新配置文件中的purpose,生成更新后的param，为后续使用
                 "time_type": time_type}
    toml_lists = ['/forecast/time_series/config/sales_data.toml', '/forecast/time_series/config/model.toml',
                  '/forecast/time_series/config/operation.toml']
    seactions = ['data', None, None]
    param = {}
    for i in zip(toml_lists, seactions):
        params = get_config(os.getcwd() + i[0], i[1])
        param.update(params)
    param.update(param_cur)
    return param


def run(forecast_start_date, purpose, time_type, spark):
    """
    跑接口
    :return:
    """
    status = True
    setup_console_log()
    setup_logging(info_log_file="predict.info", error_log_file="", info_log_file_level="INFO")
    logging.info("LOADING···")
    param = load_params(forecast_start_date, purpose, time_type)
    logging.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logging.info("RUNNING···")
            status = predict_sp(param, spark)
        else:
            # pandas版本
            pass
        # status = "SUCCESS"
        logging.info(status)
    except Exception as e:
        status = "ERROR"
        logging.info(traceback.format_exc())
    return status


if __name__ == "__main__":
    files1 = zipfile.ZipFile('./forecast.zip', 'r')
    files2 = zipfile.ZipFile('./digitforce.zip', 'r')
    files1.extractall(os.getcwd())
    files2.extractall(os.getcwd())
    spark = forecast_spark_session("wf_submit_test")
    forecast_start_date, purpose, time_type = sys.argv[1].replace('-', ''), sys.argv[2], sys.argv[3],
    run(forecast_start_date, purpose, time_type, spark)
