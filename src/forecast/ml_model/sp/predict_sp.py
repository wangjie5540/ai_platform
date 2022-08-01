# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    机器学习模型：预测spark版本
"""

import os
import sys
from zipfile import ZipFile
import shutil
from digitforce.aip.common.file_config import get_config
from digitforce.aip.common.data_helper import update_param_default
from forecast.common.spark import spark_init
from forecast.ml_model.sp.data_prepare import *
from forecast.ml_model.model.ml_predict import ml_predict
# from forecast.common.save_data import write_to_hive
# from digitforce.aip.common.spark_helper import SparkHelper,forecast_spark_session
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
import logging

logger_info = setup_console_log()
setup_logging(info_log_file="sales_fill_zero.info", error_log_file="", info_log_file_level="INFO")


# file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
# sys.path.append(file_path)  # 解决不同位置调用依赖包路径问题


def get_default_conf():
    """
    获取时序预测所需的默认参数
    :return: 默认参数
    """
    file_tmp = "forecast/ml_model/config/"
    ml_model_operation = file_tmp + 'operation.toml'
    ml_model = file_tmp + r'model.toml'
    sales_data_file = file_tmp + r'sales_data.toml'
    if os.path.exists(file_tmp):  # 如果压缩文件存在，是为了兼顾spark_submit形式
        try:
            dst_dir = os.getcwd() + '/zip_tmp'
            zo = ZipFile(file_tmp, 'r')
            if os.path.exists(dst_dir):
                shutil.rmtree(dst_dir)
            os.mkdir(dst_dir)
            for file in zo.namelist():
                zo.extract(file, dst_dir)
            ml_model_operation = ml_model_operation  # 解压后的地址
            ml_model = ml_model  # 解压后的地址
            sales_data_file = sales_data_file
        except:
            ml_model_operation = ml_model_operation  # 解压后的地址
            ml_model = ml_model  # 解压后的地址
            sales_data_file = sales_data_file

    conf_default = get_config(ml_model_operation, 'default')
    result_processing_param = get_config(ml_model_operation, 'result_processing_param')
    method_param_all = get_config(ml_model)  # 模型参数
    conf_default['method_param_all'] = method_param_all
    conf_default['result_processing_param'] = result_processing_param
    sales_data_dict = get_config(sales_data_file, 'data')
    conf_default.update(sales_data_dict)

    try:
        shutil.rmtree(dst_dir)
        zo.close()
    except:
        pass
    return conf_default


def key_process(x, key_list):
    """
    根据key_list生成key值
    :param x: value值
    :param key_list: key的列表
    :return:key值的元数组
    """
    return tuple([x[key] for key in key_list])


def is_exist_table(spark, check_table):
    """
    判断表是否存在
    :param spark:
    :param check_table: 要检查的表
    :return:
    """
    result = False
    try:
        if spark.table("{0}".format(check_table)):
            result = True
    except:
        pass
    return result


def show_columns(spark, check_table):
    columns = spark.sql("show columns in {0}".format(check_table)).toPandas()['col_name'].tolist()
    return columns


def save_table(spark, sparkdf, table_name, save_mode='overwrite', partition=["shop_id", "dt"]):
    if is_exist_table(spark, table_name):
        columns = show_columns(spark, table_name)
        print(columns, table_name)
        sparkdf.repartition(1).select(columns).write.mode("overwrite").insertInto(table_name, True)
    else:
        print("save table name", table_name)
        logging.info("save_table begin")
        sparkdf.write.mode(save_mode).partitionBy(partition).saveAsTable(table_name)


def method_called_predict_sp(data, key_cols, hdfs_path, param, predict_len):
    """
    模型预测
    :param data: 预测特征
    :param key_cols: key的列
    :param hdfs_path: 保存hdfs地址
    :param param: 参数
    :param predict_len: 预测时长
    :return: 预测值
    """
    try:
        back_testing = param['back_testing']
    except:
        back_testing = None
    forcast_start_date = param['forcast_start_date']
    if predict_len <= 0:
        return
    data_result = data.rdd.map(lambda g: (key_process(g, key_cols), g)).groupByKey(). \
        flatMap(lambda x: ml_predict(x[0], x[1], predict_len,forcast_start_date, hdfs_path, param, 'sp',back_testing)).filter(
        lambda h: h is not None).toDF()
    return data_result


def predict_sp(param, spark):
    """
    机器学习模型的预测
    :param param: 参数
    :param spark: spark
    :return:
    """
    status = True

    if 'purpose' not in param.keys() or 'predict_len' not in param.keys():
        logging.info('problem:purpose or predict_len')
        return False
    if param['purpose'] != 'predict':
        logging.info('problem:purpose is not predict')
        return False
    if param['predict_len'] < 0 or param['predict_len'] == '':
        logging.info('problem:predict_len is "" or predict_len<0')
        return False
    default_conf = get_default_conf()
    param = update_param_default(param, default_conf)
    logging.info("ml_time_operation:")
    logging.info(str(param))
    mode_type = param['mode_type']
    spark_inner = 0
    if str(mode_type).lower() == 'sp' and not spark:
        try:
            # forecast_spark_helper = SparkHelper(forecast_spark_session("forecast_app"))
            # spark = forecast_spark_helper.get_spark() # spark_init()
            logging.info('spark 启动成功')
            status = False
        except Exception as e:
            logging.info(traceback.format_exc())
        spark_inner = 1
    key_cols = param['col_keys']
    predict_len = param['predict_len']
    result_processing_param = param['result_processing_param']
    hdfs_path = param['hdfs_path']

    try:
        data_predict = data_prepare_predict(spark, param)  # 预测样本
        data_pred = method_called_predict_sp(data_predict, key_cols, hdfs_path, param, predict_len)
        status = False
    except Exception as e:
        data_pred = None
        logging.info(traceback.format_exc())
    if data_pred is not None:  # 预测和回测的结果写表
        try:
            partition = result_processing_param['partition']
            table_name = result_processing_param['table_name']
            mode_type = result_processing_param['mode_type']
            save_table(spark, data_pred, table_name, mode_type, partition=["shop_id", "dt"])  # 结果保存
            # write_to_hive(spark, data_pred, partition, table_name, mode_type)  # 结果保存
            logging.info('result_processing_sp 成功')
        except Exception as e:
            logging.info(traceback.format_exc())

    if spark_inner == 1:  # 如果当前接口启动的spark，那么要停止
        spark.stop()
        logging.info("spark stop")
    return status
