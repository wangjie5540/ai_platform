# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    各种机器学习模型进行回测
"""
import pandas as pd

from forecast.ml_model.model.LightgbmModel import LightgbmModel
from forecast.ml_model.model.XgboostModel import XgboostModel
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from forecast.common.model_helper import *
from digitforce.aip.common.data_helper import *
from pyspark.sql import Row
from forecast.ml_model.model.ml_train import ml_train
from forecast.ml_model.model.ml_predict import ml_predict


def ml_back_test(key_value, data_all, method, param, save_path,
                 predict_len, mode_type, back_testing=None):
    """

    :param key_value:
    :param data_all:
    :param method:
    :param param:
    :param save_path:
    :param predict_len:
    :param mode_type:
    :param back_testing:
    """
    predict_sum = 0
    step_len = param['step_len']

    result_data = None
    if predict_len <= 0:
        return result_data
    if step_len <= 0:
        step_len = 1
    time_col = param['time_col']  # 表示时间的的列，例如：dt
    if isinstance(data_all, pd.DataFrame):
        data = data_all
    else:

        data = row_transform_to_dataFrame(data_all)
    bt_sdate = '20210110'
    bt_len = 30
    time_type = param['time_type']
    dict_time_type = {'day': 'D', 'week': 'W', 'month': 'M'}
    bt_date_list = [x.strftime(format='%Y%m%d') for x
                    in pd.date_range(bt_sdate, periods=30, freq=dict_time_type[time_type])]
    result_data = pd.DataFrame()
    for x in zip(range(bt_len), bt_date_list):

        predict_sum += step_len
        bt_date = x[1]
        data_tmp = data[data[time_col] <= bt_date]
        data_tmp['rank'] = data_tmp.groupby(list(key_value))[time_col].transform('rank', method='first',
                                                                           ascending=False)

        print("predict_sum is ", predict_sum, "i is ", x[0])
        if predict_sum == step_len:
            ml_train(key_value, data_tmp, method, param, save_path,
                     predict_len, mode_type, back_testing)  # 训练模型
            result_tmp = ml_predict(key_value, data_tmp[data_tmp['rank'] == 1], predict_len, save_path, param,
                                    mode_type, back_testing)  # 模型预测
        elif predict_sum > predict_len:
            tmp_len = predict_len + step_len - predict_sum
            predict_sum = tmp_len
            ml_train(key_value, data_tmp, method, param, save_path,
                     predict_len, mode_type, back_testing)  # 训练模型
            result_tmp = ml_predict(key_value, data_tmp[data_tmp['rank'] == 1], predict_len, save_path, param,
                                    mode_type, back_testing)  # 模型预测
        else:
            result_tmp = ml_predict(key_value, data_tmp[data_tmp['rank'] == 1], predict_len, save_path, param,
                                    mode_type, back_testing)  # 模型预测
        result_data = pd.concat(result_data, result_tmp)

    if not isinstance(data_all, pd.DataFrame):
        resultRow = Row(*result_data.columns)
        data_result = []
        for r in result_data.values:
            data_result.append(resultRow(*r))
        data_result = data_result  # [save_table_cols]
    else:
        data_result = result_data
    return data_result
