# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：回测spark版本
"""

from forecast.time_series.sp.predict_for_time_serise_sp import method_called_predict_sp
from forecast.time_series.sp.data_prepare_for_time_series_sp import *
from forecast.common.common_helper import *
from forecast.model_evaluation import forecast_evaluation
from forecast.time_series.sp.data_prepare_for_time_series_sp import data_prepared_for_model


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
    output_table = param['output_table']
    partitions = param['partitions']
    dt = param['time_col']

    spark_df = data_prepared_for_model(spark, param)

    # 按照forecast_start_time将数据集划分为训练集和测试集
    back_test_data = spark_df.filter(spark_df[dt] >= forecast_start_date)

    back_end_date = back_test_data.select(dt).rdd.max()[0]  # 回测期获取最大值
    back_end_date = datetime.datetime.strftime(back_end_date, "%Y%m%d")
    index = pd.date_range(forecast_start_date, back_end_date, freq='D')
    temp_dict = {"day": "D", "week": "W-MON", "month": "MS", "season": "QS-OCT", "year": "A"}
    if param['time_type'] in temp_dict:
        index = pd.date_range(forecast_start_date, back_end_date, freq=temp_dict[param['time_type']])

    time_list = list(datetime.datetime.strftime(i, "%Y%m%d") for i in index)

    # TODO 每天都过滤还是一次性过率好那个效果更好？
    for cur_time in time_list:
        if cur_time == forecast_start_date:
            result_data_temp = method_called_predict_sp(param, spark_df, cur_time)
        else:
            result_data_temp = result_data_temp.union(method_called_predict_sp(param, spark_df, cur_time))

    key_cols.append(dt)
    back_test_data = back_test_data.join(result_data_temp, on=key_cols, how='left')
    param['forecast_start_date'] = forecast_start_date
    save_table(spark, back_test_data, output_table, partition=partitions)
    wmape_spdf = forecast_evaluation.forecast_evaluation_wmape(back_test_data, col_qty, "pred_time", col_key=key_cols,
                                                               df_type='sp')
    get_logger().info("回测效果", wmape_spdf)



def back_test_sp(param, spark):
    """
    时序模型运行
    :param param: 参数
    :param spark: spark
    :return:
    """
    logger_info = get_logger()
    if 'purpose' not in param.keys() or 'predict_len' not in param.keys():
        logger_info.info('problem:purpose or predict_len')
        return False
    if param['purpose'] != 'back_test':
        logger_info.info('problem:purpose is not predict')
        return False
    if param['predict_len'] < 0 or param['predict_len'] == '':
        logger_info.info('problem:predict_len is "" or predict_len<0')
        return False
    logger_info.info("time_series_operation:")
    logger_info.info(str(param))
    status = method_called_back_sp(spark, param)
    return status
