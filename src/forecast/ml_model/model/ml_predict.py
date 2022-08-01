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
from digitforce.aip.common.data_helper import *


def ml_predict(key_value, predict_x, predict_len, predict_dt, save_path, param, mode_type, back_testing=None):
    """
    模型训练
    :param key_value: key的值
    :param predict_x: 预测特征
    :param predict_len:预测时长
    :param predict_dt: 预测日期（跑模型日期）
    :param save_path: 模型保存地址
    :param param: 参数
    :param mode_type: 运行方式
    :param back_testing: 是否进行回测
    :return: 预测值
    """
    if isinstance(predict_x, pd.DataFrame):
        data = predict_x
    else:
        data = row_transform_to_dataFrame(predict_x)
    print("param is ml_predict is ", str(param))

    time_type = param['time_type']
    save_table_cols = param['save_table_cols']
    cols_feat_x_columns = param['cols_feat_x_columns']  # 模型训练特征
    time_col = param['time_col']
    model_name = 'ml'
    is_log = True
    if 'is_log' in param.keys():
        is_log = param['is_log']
    if isinstance(key_value, list):
        key_value = '/'.join(key_value)
    elif isinstance(key_value, tuple):
        key_value_list = [str(i) for i in list(key_value)]
        key_value = '/'.join(key_value_list)
    save_path = save_path + '/' + key_value
    if mode_type == 'sp' and not back_testing:
        model_all = load_model_hdfs(model_name, save_path, key_value)  # 加载模型
    else:
        file_local = r'model_tmp/' + key_value  #
        file_local_tmp = file_local + '/' + model_name
        model_all = load_model(file_local_tmp)
    labels_list = ["y{}".format(i) for i in range(1, predict_len + 1)]
    days_all = [i for i in range(1, predict_len + 1)]
    result_df_all = []
    for label, day in zip(labels_list, days_all):
        result_df = pd.DataFrame()
        predict_data = data[cols_feat_x_columns]
        model_lgb = model_all[str(label)]
        y_pred = model_lgb.predict(predict_data)
        if is_log == True:
            y_pred = np.expm1(y_pred)
        for col in save_table_cols:
            if col not in ['pred_time', 'y_pred', 'time_type']:  # 固定列
                result_df[col] = data[col]
        result_df['pred_time'] = day
        result_df['y_pred'] = y_pred
        result_df['time_type'] = time_type
        result_df[time_col] = predict_dt
        result_df_all.append(result_df)
    result_df_all = pd.concat(result_df_all)  # 结果进行合并
    result_df_all = result_df_all[save_table_cols]
    if not isinstance(predict_x, pd.DataFrame):
        resultRow = Row(*result_df_all.columns)
        data_result = []
        for r in result_df_all.values:
            data_result.append(resultRow(*r))
        data_result = data_result  # [save_table_cols]
    else:
        data_result = result_df_all
    return data_result
#     if mode_type == 'sp':
#         resultRow = Row(*result_df_all.columns)
#         data_result = []
#         for r in result_df_all.values:
#             data_result.append(resultRow(*r))
#         data_result = data_result  # [save_table_cols]
#     else:
#         data_result = result_df_all
#     return data_result

