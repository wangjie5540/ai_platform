# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：回测spark版本
"""
import datetime
import logging

import pandas as pd
from pyspark.sql.functions import lit

from forecast.time_series.sp.predict_for_time_series_sp import method_called_predict_sp
from forecast.model_evaluation import forecast_evaluation
from forecast.time_series.sp.data_prepare_for_time_series_sp import data_prepared_for_model
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.spark_helper import save_table


def method_called_back_sp(spark, param):
    """
    模型回测
    :param data: 样本
    :param key_cols: FlatMap使用key
    :param apply_model_index: 模型在key_cols中的位置
    :param param: 参数集合
    :param forcast_start_date: 预测开始日期
    :param predict_len: 预测时长
    :param step_len: 回测时每次预测步长
    :param assist_param: 一些辅助函数
    :return: 回测结果
    """
    key_cols = param['key_cols']
    forecast_start_date = param['forecast_start_date']
    col_qty = param['col_qty']
    back_test_table = param['back_test_table']
    partitions = param['partitions']
    dt = param['time_col']
    eval_key = param['eval_key']
    eval_table = param['eval_table']
    prepare_data_table = param['prepare_data_table']

    spark_df = spark.table(prepare_data_table)
        # data_prepared_for_model(spark, param)

    # 按照forecast_start_time将数据集划分为训练集和测试集
    back_test_data = spark_df.filter(spark_df[dt] >= forecast_start_date)

    back_end_date = back_test_data.select(dt).rdd.max()[0]  # 回测期获取最大值

    temp_dict = {"day": "D", "week": "W-MON", "month": "MS", "season": "QS-OCT", "year": "A"}
    if param['time_type'] in temp_dict:
        index = pd.date_range(forecast_start_date, back_end_date, freq=temp_dict[param['time_type']])
    else:
        index = pd.date_range(forecast_start_date, back_end_date, freq='D')

    time_list = list(datetime.datetime.strftime(i, "%Y%m%d") for i in index)

    # TODO 每天都过滤还是一次性过率好那个效果更好？
    i = 0
    for cur_time in time_list:
        if i == 0:
            result_data_temp = method_called_predict_sp(param, spark_df, cur_time)
            i += 1
        else:
            result_data_temp = result_data_temp.union(method_called_predict_sp(param, spark_df, cur_time))

    key_cols.append(dt)
    back_test_data = back_test_data.join(result_data_temp, on=key_cols, how='left')
    save_table(spark, back_test_data, back_test_table, partition=partitions)
    wmape_spdf = forecast_evaluation.forecast_evaluation_wmape(back_test_data, col_qty, "y_pred", col_key=eval_key,
                                                               df_type='sp')
    apply_model = back_test_data.select("apply_model").rdd.max()[0]
    wmape_spdf.withColumn("apply_model", lit(apply_model))
    wmape_spdf.withColumn("time_type",lit(param['time_type']))
    save_table(spark,wmape_spdf,eval_table,partition=eval_key)
    print("回测效果", wmape_spdf.show(10))


def back_test_sp(param, spark):
    """
    时序模型运行
    :param param: 参数
    :param spark: spark
    :return:
    """
    setup_console_log(level=logging.INFO)
    setup_logging(info_log_file="backup_test_for_time_series_sp.info", error_log_file="", info_log_file_level="INFO")
    if 'purpose' not in param.keys() or 'predict_len' not in param.keys():
        logging.info('problem:purpose or predict_len')
        return False
    if param['purpose'] != 'back_test':
        logging.info('problem:purpose is not predict')
        return False
    if param['predict_len'] < 0 or param['predict_len'] == '':
        logging.info('problem:predict_len is "" or predict_len<0')
        return False
    status = method_called_back_sp(spark, param)
    return status

# def model_select(spark, param):
#     """
#     模型回测
#     :param data: 样本
#     :param key_cols: FlatMap使用key
#     :param apply_model_index: 模型在key_cols中的位置
#     :param param: 参数集合
#     :param forcast_start_date: 预测开始日期
#     :param predict_len: 预测时长
#     :param step_len: 回测时每次预测步长
#     :param assist_param: 一些辅助函数
#     :return: 回测结果
#     """
#     model_list = ['arima','holt_winter']
#     key_cols = param['key_cols']
#     forecast_start_date = param['forecast_start_date']
#     col_qty = param['col_qty']
#     output_table = param['output_table']
#     partitions = param['partitions']
#     dt = param['time_col']
#     eval_key = param['eval_key']
#     eval_table = param['eval_table']
#
#
#     spark_df = data_prepared_for_model(spark, param)
#
#     # 按照forecast_start_time将数据集划分为训练集和测试集
#     back_test_data = spark_df.filter(spark_df[dt] >= forecast_start_date)
#
#     back_end_date = back_test_data.select(dt).rdd.max()[0]  # 回测期获取最大值
#
#     temp_dict = {"day": "D", "week": "W-MON", "month": "MS", "season": "QS-OCT", "year": "A"}
#     if param['time_type'] in temp_dict:
#         index = pd.date_range(forecast_start_date, back_end_date, freq=temp_dict[param['time_type']])
#     else:
#         index = pd.date_range(forecast_start_date, back_end_date, freq='D')
#
#     time_list = list(datetime.datetime.strftime(i, "%Y%m%d") for i in index)
#
#     i = 0
#     for cur_time in time_list:
#         if i == 0:
#             result_data_temp = method_called_predict_sp(param, spark_df, cur_time)
#             i += 1
#         else:
#             result_data_temp = result_data_temp.union(method_called_predict_sp(param, spark_df, cur_time))
#
#     key_cols.append(dt)
#     back_test_data = back_test_data.join(result_data_temp, on=key_cols, how='left')
#     save_table(spark, back_test_data, output_table, partition=partitions)
#     wmape_spdf = forecast_evaluation.forecast_evaluation_wmape(back_test_data, col_qty, "y_pred", col_key=eval_key,
#                                                                df_type='sp')
#     apply_model = back_test_data.select("apply_model").rdd.max()[0]
#     wmape_spdf.withColumn("apply_model", lit(apply_model))
#     wmape_spdf.withColumn("time_type",lit(param['time_type']))
#     save_table(spark,wmape_spdf,eval_table,partition=eval_key)
#     print("回测效果", wmape_spdf.show(10))
