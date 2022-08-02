# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：预测spark版本
"""

from forecast.time_series.model import ARModel, ARXModel, ARIMAXModel, ARIMAModel, ThetaModel, SARIMAXModel, \
    MAModel, SARIMAModel, SESModel, CrostonModel, CrostonTSBModel, HoltModel, HoltWinterModel, \
    STLForecastModel
from digitforce.aip.common.data_helper import *
from forecast.time_series.sp.data_prepare_for_time_series_sp import data_process

'''
整体思路：
1，获得输入参数，通过参数解析选择模型
2，数据准备
3，模型预测
4，效果评估
5，结果存储
'''


def model_predict(key_value, data, method, param, forecast_start_date, predict_len, mode_type):
    """
    所有的时序模型预测,可以实现pipeline和spark级别并行
    :param key_value: key值
    :param data: 样本
    :param method: 选择方法
    :param key_cols: key值的关键字
    :param param: 参数集合
    :param forecast_start_date: 预测开始日期
    :param predict_len: 预测时长
    :param mode_type: 运行方式
    :return: 预测结果
    """
    method_param_all = param['method_param_all']
    time_col = param['time_col']
    time_type = param['time_type']
    save_table_cols = param['default']['save_table_cols']
    key_cols = param['key_cols']
    y = param['col_qty']
    model_include = True
    data_temp = row_transform_to_dataFrame(data)

    # 天维度连续性检测
    data = data_temp
    if param['time_type'] == 'day':
        data = data_process(data_temp, forecast_start_date, time_col, y)

    temp_dict = {"day": "D", "week": "W-MON", "month": "MS", "season": "QS-OCT", "year": "A"}
    method_param = method_param_all[method]

    if param['time_type'] in temp_dict:
        index = pd.date_range(forecast_start_date, periods=predict_len, freq=temp_dict[param['time_type']])
    else:
        index = pd.date_range(forecast_start_date, periods=predict_len, freq="D")

    data_tmp = data[data[time_col] < forecast_start_date]  # 日期小于预测日期
    data_tmp = data_tmp.sort_values(by=time_col, ascending=True)  # 进行排序

    data_tmp[time_col] = data_tmp[time_col].apply(lambda x: pd.to_datetime(x))
    p_data = data_tmp[[y, time_col]].set_index(time_col)
    p_data[y] = p_data[y].astype(float)

    if str(method).lower() == 'holt-winter':
        seasonal_periods = method_param["param"]["seasonal_periods"]
        if p_data.shape[0] > max(2 * seasonal_periods, (10 + 2 * (seasonal_periods // 2))):
            ts_model = HoltWinterModel.HoltWinterModel(p_data, param=method_param["param"]).fit()
        else:
            ts_model = SESModel.SESModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'holt':
        if p_data.shape[0] >= 10:
            ts_model = HoltModel.HoltModel(p_data, param=method_param["param"],
                                           param_fit=method_param["param_fit"]).fit()
        else:
            ts_model = SESModel.SESModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'ar':
        params = method_param['param']
        params_fit = method_param['param_fit']
        lags = params['lags']
        if p_data.shape[0] > 2 * lags + 1:
            ts_model = ARModel.ARModel(p_data, param=params, param_fit=params_fit).fit()
        else:
            ts_model = SESModel.SESModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'ma':
        ts_model = MAModel.MAModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'arx':
        if p_data.shape[0] > 3 * method_param['param']['lags']:
            ts_model = ARXModel.ARXModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
        else:
            ts_model = SESModel.SESModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'arima':
        ts_model = ARIMAModel.ARIMAModel(p_data, param=method_param["param"],
                                         param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'arimax':
        ts_model = ARIMAXModel.ARIMAXModel(p_data, param=method_param["param"],
                                           param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'sarima':
        ts_model = SARIMAModel.SARIMAModel(p_data, param=method_param["param"],
                                           param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'sarimax':
        ts_model = SARIMAXModel.SARIMAXModel(p_data, param=method_param["param"],
                                             param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'ses':
        if method_param['param']['initialization_method'] != 'legacy-heuristic':
            if p_data.shape[0] >= 10:
                ts_model = SESModel.SESModel(p_data, param=method_param["param"],
                                             param_fit=method_param["param_fit"]).fit()
            else:
                method_param['param']['initialization_method'] = 'legacy-heuristic'
                ts_model = SESModel.SESModel(p_data, param=method_param["param"],
                                             param_fit=method_param["param_fit"]).fit()
        else:
            ts_model = SESModel.SESModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'croston':
        ts_model = CrostonModel.CrostonModel(p_data, param=method_param["param"]).fit()
    elif str(method).lower() == 'crostontsb':
        ts_model = CrostonTSBModel.CrostonTSBModel(p_data, param=method_param["param"]).fit()
    elif str(method).lower() == 'theta':
        ts_model = ThetaModel.ThetaModels(p_data, param=method_param["param"],
                                          param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'stlf':
        ts_model = STLForecastModel.STLFModel(p_data, param=method_param["param"],
                                              param_fit=method_param["param_fit"]).fit()
    else:
        ts_model = None

    result_df = pd.DataFrame()
    preds = ts_model.forecast(predict_len)
    dict_ = {'datetime': preds.index, 'y': preds.values}
    df_ = pd.DataFrame(dict_)
    result_df['y_pred'] = df_['y']
    result_df['dt'] = [i for i in range(len(index))]
    result_df['time_type'] = time_type
    result_df['pred_time'] = forecast_start_date
    result_df['y_pred'] = result_df['y_pred'].apply(lambda x: x if x >= 0 else 0)

    data_result = predict_result_handle(result_df, key_value, key_cols, mode_type, save_table_cols)  # 对结果进行处理
    return data_result
