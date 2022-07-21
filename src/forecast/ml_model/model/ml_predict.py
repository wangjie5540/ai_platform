# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    机器学习模型预测
"""
import numpy as np
import pandas as pd
from forecast.common.model_helper import *
from forecast.common.data_helper import *


def ml_predict(key_value, predict_x, predict_len, save_path, param, mode_type):
    """
    模型训练
    :param key_value: key的值
    :param predict_x: 预测特征
    :param predict_len:预测时长
    :param save_path: 模型保存地址
    :param param: 参数
    :param mode_type: 运行方式
    :return: 预测值
    """
    time_type = param['time_type']
    save_table_cols = param['save_table_cols']
    cols_feat_x_columns = param['cols_feat_x_columns']  # 模型训练特征
    model_name = 'ml'
    is_log = True
    if 'is_log' in param.keys():
        is_log = param['is_log']
    if isinstance(key_value, list):
        key_value = '/'.join(key_value)
    save_path = save_path + '/' + key_value
    if mode_type == 'sp':
        model_all = load_model_hdfs(model_name, save_path)  # 加载模型
    else:
        save_path = save_path + '/' + model_name
        model_all = load_model(save_path)
    labels_list = ["y{}".format(i) for i in range(1, predict_len + 1)]
    days_all = [i for i in range(1, predict_len + 1)]
    result_df_all = []
    for label, day in zip(labels_list, days_all):
        result_df = pd.DataFrame()
        predict_data = predict_x[cols_feat_x_columns]
        model_lgb = model_all[str(label)]
        y_pred = model_lgb.predict(predict_data)
        if is_log == True:
            y_pred = np.expm1(y_pred)
        for col in save_table_cols:
            if col not in ['pred_time', 'y_pred', 'time_type']:  # 固定列
                result_df[col] = predict_x[col]
        result_df['pred_time'] = day
        result_df['y_pred'] = y_pred
        result_df['time_type'] = time_type
        result_df_all.append(result_df)
    result_df_all = pd.concat(result_df_all)  # 结果进行合并
    if mode_type == 'sp':
        resultRow = Row(*result_df_all.columns)
        data_result = []
        for r in result_df_all.values:
            data_result.append(resultRow(*r))
        data_result = data_result[save_table_cols]
    else:
        data_result = result_df_all
    return data_result
