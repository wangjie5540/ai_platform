# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：预测spark版本
"""
from forecast.time_series.sp.model_predict import *


def key_process(x, key_cols):
    """
    根据key_cols生成key值
    :param x: value值
    :param key_cols: key的列表
    :return:key值的元数组
    """
    return tuple([x[key] for key in key_cols])


def method_called_predict_sp(param, spark_df, cur_time):
    """
    模型调用
    :param data:样本
    :param key_cols:FlatMap使用key
    :param apply_model_index: 模型在key_cols中的位置
    :param param: 参数集合
    :param forecast_start_date: 预测开始日期
    :param predict_len: 预测时长
    :return: 预测后的结果
    """
    key_cols = param['key_cols']
    apply_model_index = param['apply_model_index']
    forecast_start_date = param['forecast_start_date']
    predict_len = param['predict_len']
    if cur_time == forecast_start_date:
        pass
    else:
        forecast_start_date = cur_time

    if predict_len <= 0:
        return
    data_result = spark_df.rdd.map(lambda g: (key_process(g, key_cols), g)).groupByKey(). \
        flatMap(lambda x: model_predict(x[0], x[1], x[0][apply_model_index], param, forecast_start_date, predict_len,
                                        'sp')).filter(lambda h: h is not None).toDF()

    return data_result


def predict_sp(param, spark):
    """
    时序模型运行
    :param param: 参数
    :param spark: spark
    :return:
    """
    # s数据准备是否成功
    prepare_data = data_prepared_for_model(spark, param)
    output_table = param['output_table']
    partitions = param['partitions']
    preds = method_called_predict_sp(param, prepare_data)
    save_table(spark, preds, output_table, partition=partitions)
    status = "TEST SUCCESS"

    return status
