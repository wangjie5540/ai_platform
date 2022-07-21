# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    机器学习模型：回测spark版本
"""
import os
import sys
import traceback
from forecast.common.log import get_logger
from forecast.common.date_helper import date_add_str
from forecast.ml_model.sp.predict_sp import method_called_predict_sp, get_default_conf
from forecast.common.data_helper import update_param_default
from forecast.common.spark import spark_init
from forecast.ml_model.sp.data_prepare import data_prepare_train
from forecast.common.save_data import write_to_hive
from forecast.ml_model.sp.train_sp import method_called_train_sp

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(file_path)  # 解决不同位置调用依赖包路径问题


def method_called_back_sp(data, key_cols, apply_model_index, forcast_start_date, predict_len, param, hdfs_path,
                          step_len):
    """
    模型回测
    :param data: 样本
    :param key_cols: key值的列
    :param apply_model_index: apply_model的index
    :param forcast_start_date: 预测开始时间
    :param predict_len: 预测时长
    :param param: 参数
    :param hdfs_path: 模型保存hdfs地址
    :param step_len: 回测步长
    :return: 回测结果
    """
    predict_sum = 0
    time_type = param['time_type']
    time_col = param['time_col']
    result_data = None
    if predict_len <= 0:
        return result_data
    if step_len <= 0:
        step_len = 1
    for i in range(predict_len):
        if i != 0:
            forcast_start_date = date_add_str(forcast_start_date, step_len, time_type)
        predict_sum += step_len
        data_train = data.filter((data[time_col] < forcast_start_date))
        data_predict = data.filter(data[time_col] == forcast_start_date)
        if predict_sum > predict_len:
            tmp_len = predict_len + step_len - predict_sum
            method_called_train_sp(data_train, key_cols, apply_model_index, param, hdfs_path, tmp_len)  # 训练模型
            result_tmp = method_called_predict_sp(data_predict, key_cols, hdfs_path, param, tmp_len)  # 模型预测
        else:
            method_called_train_sp(data_train, key_cols, apply_model_index, param, hdfs_path, step_len)  # 训练模型
            result_tmp = method_called_predict_sp(data_predict, key_cols, hdfs_path, param, step_len)  # 模型预测
        if i == 0:
            result_data = result_tmp
        else:
            result_data = result_data.union(result_tmp)  # 合并结果
        if predict_sum > predict_len:
            break
    return result_data


def back_test_sp(param, spark):
    """
    机器学习模型的预测和回测
    :param param: 参数
    :param spark: spark
    :return:
    """
    status = True
    logger_info = get_logger()
    if 'purpose' not in param.keys() or 'predict_len' not in param.keys():
        logger_info.info('problem:purpose or predict_len')
        return False
    if param['purpose'] != 'back_test':
        logger_info.info('problem:purpose is not back_test')
        return False
    if param['predict_len'] < 0 or param['predict_len'] == '':
        logger_info.info('problem:predict_len is "" or predict_len<0')
        return False
    default_conf = get_default_conf()
    param = update_param_default(param, default_conf)
    logger_info.info("ml_time_operation:")
    logger_info.info(str(param))
    mode_type = param['mode_type']
    spark_inner = 0
    if str(mode_type).lower() == 'sp' and not spark:
        try:
            spark = spark_init()
            logger_info.info('spark 启动成功')
        except Exception as e:
            status = False
            logger_info.info(traceback.format_exc())
        spark_inner = 1
    key_cols = param['key_cols']
    apply_model_index = param['apply_model_index']
    forcast_start_date = param['forcast_start_date']
    predict_len = param['predict_len']
    step_len = param['step_len']
    result_processing_param = param['result_processing_param']
    hdfs_path = param['hdfs_path']

    try:
        data_train = data_prepare_train(spark, param)  # 训练样本
        data_pred = method_called_back_sp(data_train, key_cols, apply_model_index, forcast_start_date, predict_len,
                                          param, hdfs_path, step_len)
    except Exception as e:
        data_pred = None
        status = False
        logger_info.info(traceback.format_exc())
    if data_pred is not None:  # 预测和回测的结果写表
        try:
            partition = result_processing_param['partition']
            table_name = result_processing_param['table_name']
            mode_type = result_processing_param['mode_type']
            write_to_hive(spark, data_pred, partition, table_name, mode_type)  # 结果保存
        except Exception as e:
            status = False
            logger_info.info(traceback.format_exc())

    if spark_inner == 1:  # 如果当前接口启动的spark，那么要停止
        spark.stop()
        logger_info.info("spark stop")
    return status
