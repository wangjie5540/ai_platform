# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    各种机器学习模型
"""
from forecast.ml_model.model.LightgbmModel import LightgbmModel
from forecast.ml_model.model.XgboostModel import XgboostModel
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from forecast.common.model_helper import *
from digitforce.aip.common.data_helper import *
from pyspark.sql import Row


def generate_rows_from_df(df, cast_int=None):
    columns = df.columns.tolist()
    row_list = list()
    for row in df.iterrows():
        k_v = dict()
        for key in columns:
            if cast_int is not None and key in cast_int:
                k_v[key] = int(row[1][key])
            else:
                k_v[key] = row[1][key]
        row_list.append(Row(**k_v))
    return row_list


def ml_train(key_value, data_all, method, param, save_path, predict_len, mode_type,back_testing=None):
    """
    模型训练
    :param key_value: key值
    :param data_all: 样本
    :param method: 方法
    :param param: 参数
    :param save_path: 模型上保存地址
    :param predict_len: 预测时长
    :param mode_type: 运行方式
    :return:
    """
    print("key_value",key_value)
    method_param_all = param['method_param_all']
    try:
        method_param = method_param_all[method]
    except:
        method_param = {}

    is_log = True  # 是否进行log变换
    if 'is_log' in param.keys():
        is_log = param['is_log']

    time_col = param['time_col']  # 表示时间的的列，例如：dt
    time_type = param['time_type']  # day/week/month
    y = param['cols_feat_y_columns'][0]  # y值
    loop_key = param['loop_key']  # 求y1、y2......y14的聚合key
    feature_columns = param['cols_feat_x_columns']  # 模型使用特征
    sample_join_key_feat = param['sample_join_key_feat']
    model_name = 'ml'
    data = row_transform_to_dataFrame(data_all)

    edate = data[time_col].max()
    sdate = datetime.datetime.strptime(edate, '%Y%m%d')
    if time_type == 'day':
        delta_n = datetime.timedelta(days=-predict_len)
        feature_sdate = sdate + delta_n
        feature_date = [x.strftime('%Y%m%d') for x in pd.date_range(feature_sdate, periods=predict_len)]
    elif time_type == 'week':
        delta_n = datetime.timedelta(weeks=-predict_len)
        feature_sdate = sdate + delta_n
        feature_date = [x.strftime('%Y%m%d') for x in pd.date_range(feature_sdate, freq='1W', periods=predict_len)]
    else:  # 月
        sdate = datetime.datetime.strptime(edate, '%Y%m%d')
        feature_sdate = sdate + relativedelta(months=-predict_len)
        feature_date = [x.strftime('%Y%m%d') for x in pd.date_range(feature_sdate, freq='1M', periods=predict_len)]
    feature_date.reverse()
    labels_list = ["y{}".format(i) for i in range(1, predict_len + 1)]
    data_loop = data.sort_values([time_col], ascending=[True])

    for i in range(1, predict_len + 1):
        data_loop['y{}'.format(i)] = data_loop.groupby(loop_key)[y].shift(-i)

    model_reg_all = {}
    importance_all = {}

    for x in zip(labels_list, feature_date):

        data_tmp = data_loop.copy()
        data_tmp = data_tmp.fillna(0)
        label = x[0]
        feature_end_date = x[1]
        df_train = data_tmp[data_tmp[time_col] < feature_end_date].copy()
        if feature_columns is None or len(feature_columns) == 0:
            train_x = df_train.drop(sample_join_key_feat)  # 全部特征
        else:
            train_x = df_train[feature_columns]
        if is_log:
            train_y = np.log1p(df_train[label])
        else:
            train_y = df_train[label]

        if method == 'lightgbm':
            model_reg = LightgbmModel(train_x, train_y, method_param)
        elif method == 'xgboost':
            model_reg = XgboostModel(train_x, train_y, method_param)
        else:
            model_reg = None

        if model_reg:

            model_reg = model_reg.fit()
            # 模型重要特征
            feature_importances = pd.DataFrame({'column': train_x.columns,
                                                'importance': model_reg.feature_importances_}
                                               ).sort_values(by='importance', ascending=False)
            model_reg_all[label] = model_reg
            importance_all[label] = feature_importances

    if save_path[-1] == '/':
        save_path = save_path[0:len(save_path) - 1]
    if isinstance(key_value, list):
        key_value = '/'.join(key_value)
    elif isinstance(key_value, tuple):
        key_value_list = [str(i) for i in list(key_value)]
        key_value = '/'.join(key_value_list)
    save_path = save_path + '/' + key_value
    print("save_path",save_path)
    print("model_name",model_name)
    print("key_value",key_value)
    if mode_type == 'sp' and not back_testing:
        save_model_hdfs(model_reg_all, model_name, save_path, key_value)
#         save_model_hdfs(model_reg_all, model_name, save_path)
    else:
        # save_path = save_path + '/' + model_name
        file_local = r'model_tmp/' + key_value  # 创建临时文件地址
        file_local_tmp = file_local + '/' + model_name
        if not os.path.exists(file_local):
            os.makedirs(file_local)
        # save_model(model, file_local_tmp)
        save_model(model_reg_all, file_local_tmp)
        print("save_model success", file_local_tmp)

    return generate_rows_from_df(data[['shop_id','goods_id']].head(1))
