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

from digitforce.aip.common.datetime_helper import date_add_str
from forecast.ml_model.sp.predict_sp import method_called_predict_sp, get_default_conf
from digitforce.aip.common.data_helper import update_param_default
from forecast.common.spark import spark_init
from forecast.ml_model.sp.data_prepare import data_prepare_train
from forecast.ml_model.sp.train_sp import method_called_train_sp
# from digitforce.aip.common.spark_helper import SparkHelper,forecast_spark_session
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
import logging
from forecast.ml_model.model.ml_backtest import ml_back_test
from forecast.ml_model.sp.data_prepare import *
logger_info = setup_console_log()
setup_logging(info_log_file="sales_fill_zero.info", error_log_file="", info_log_file_level="INFO")

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(file_path)  # 解决不同位置调用依赖包路径问题


def key_process(x, key_list):
    """
    根据key_list生成key值
    :param x: value值
    :param key_list: key的列表
    :return:key值的元数组
    """
    return tuple([x[key] for key in key_list])


def method_called_back_sp(data, key_cols, apply_model_index,
                          predict_len, back_testing_len, param, hdfs_path):
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
    :param back_testing_len: 回测总长度
    :return: 回测结果
    """

    result_data = None
    if predict_len <= 0:
        return result_data

    # 回测读取的是模型更新任务还是按照一定的周期

    try:
        back_testing = param['back_testing']
    except:
        back_testing = None

    data_result = data.rdd.map(lambda g: (key_process(g, key_cols), g)).groupByKey(). \
        flatMap(lambda x: ml_back_test(x[0], x[1], x[0][apply_model_index], param, hdfs_path,
                                       predict_len, back_testing_len, 'sp', back_testing)).filter(
        lambda h: h is not None).toDF()
    # data_result.show()

    # for i in range(back_testing_len):
    #     if i != 0:
    #         forcast_start_date = date_add_str(forcast_start_date, step_len, time_type)
    #     predict_sum += step_len
    #     data_train = data.filter((data[time_col] < forcast_start_date))
    #     data_predict = data.filter(data[time_col] == forcast_start_date)
    #     print("predict_sum is ", predict_sum, "i is ", i)
    #     if predict_sum == 1:
    #         method_called_train_sp(data_train, key_cols, apply_model_index, param, hdfs_path, step_len)  # 训练模型
    #         result_tmp = method_called_predict_sp(data_predict, key_cols, hdfs_path, param, step_len)  # 模型预测
    #     elif predict_sum > predict_len:
    #         tmp_len = predict_len + step_len - predict_sum
    #         predict_sum = tmp_len
    #         method_called_train_sp(data_train, key_cols, apply_model_index, param, hdfs_path, tmp_len)  # 训练模型
    #         result_tmp = method_called_predict_sp(data_predict, key_cols, hdfs_path, param, tmp_len)  # 模型预测
    #     else:
    #         # method_called_train_sp(data_train, key_cols, apply_model_index, param, hdfs_path, step_len)  # 训练模型
    #         result_tmp = method_called_predict_sp(data_predict, key_cols, hdfs_path, param, step_len)  # 模型预测
    #     if i == 0:
    #         result_data = result_tmp
    #     else:
    #         result_data = result_data.union(result_tmp)  # 合并结果
        # if predict_sum > predict_len:
        #     break
    return data_result
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
        sparkdf.write.mode(save_mode).partitionBy(partition).saveAsTable(table_name)



def back_test_sp(param, spark):
    """
    机器学习模型的预测和回测
    :param param: 参数
    :param spark: spark
    :return:
    """
    status = True
    if 'purpose' not in param.keys() or 'predict_len' not in param.keys():
        logging.info('problem:purpose or predict_len')
        return False
    if param['purpose'] != 'back_test':
        logging.info('problem:purpose is not back_test')
        return False
    if param['predict_len'] < 0 or param['predict_len'] == '':
        logging.info('problem:predict_len is "" or predict_len<0')
        return False
    default_conf = get_default_conf()
    param = update_param_default(param, default_conf)
    param['back_testing'] = 'bt'
    logging.info("ml_time_operation:")
    logging.info(str(param))
    mode_type = param['mode_type']
    spark_inner = 0
    if str(mode_type).lower() == 'sp' and not spark:
        try:
            spark = spark_init()
            logging.info('spark 启动成功')
        except Exception as e:
            status = False
            logging.info(traceback.format_exc())
        spark_inner = 1
    key_cols = param['col_keys']
    apply_model_index = param['apply_model_index']
    forcast_start_date = param['forcast_start_date']
    bt_sdate = param['bt_sdate']
    predict_len = param['predict_len']
    step_len = param['step_len']
    result_processing_param = param['result_processing_param']
    hdfs_path = param['hdfs_path']
    back_testing_len = 30

    try:
        group_category_select = ["16", "17", "18", "19", "20"]
        data_train = data_prepare_train(spark, param)  # 训练样本
        data_train = data_train.filter(data_train['group_category'].isin(group_category_select))
        data_pred = method_called_back_sp(data_train, key_cols, apply_model_index,
                                          predict_len, back_testing_len,
                                          param, hdfs_path)
    except Exception as e:
        data_pred = None
        status = False
        logging.info(traceback.format_exc())
    if data_pred is not None:  # 预测和回测的结果写表
        try:
            partition = result_processing_param['partition']
            table_name = result_processing_param['table_back_test_name']
            mode_type = result_processing_param['mode_type']
            save_table(spark, data_pred,  table_name, mode_type, partition=["shop_id", "dt"])  # 结果保存
        except Exception as e:
            status = False
            logging.info(traceback.format_exc())

    if spark_inner == 1:  # 如果当前接口启动的spark，那么要停止
        spark.stop()
        logging.info("spark stop")
    return status
