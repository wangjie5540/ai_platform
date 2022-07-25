# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：预测spark版本
"""

from forecast.time_series.model import ARModel, ARXModel, ARIMAXModel, ARIMAModel, ThetaModel, SARIMAXModel, \
    MAModel, SARIMAModel, SESModel, STLModel, ESModel, CrostonModel, CrostonTSBModel, HoltModel, HoltWinterModel, \
    STLForecastModel, DmsModel
from digitforce.aip.common.data_helper import *

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
        data = data_process(data_temp, param)

    temp_dict = {"day": "D", "week": "W-MON", "month": "MS", "season": "QS-OCT", "year": "A"}
    method_param = method_param_all[method]

    index = pd.date_range(forecast_start_date, periods=predict_len, freq="D")
    if param['time_type'] in temp_dict:
        index = pd.date_range(forecast_start_date, periods=predict_len, freq=temp_dict[param['time_type']])

    data_tmp = data[data[time_col] < forecast_start_date]  # 日期小于预测日期
    data_tmp = data_tmp.sort_values(by=time_col, ascending=True)  # 进行排序

    p_data = data_tmp[[y, time_col]].set_index(time_col)
    p_data[y] = p_data[y].astype(float)

    if p_data.shape[0] < 17:
        preds_value = data[y].mean()
        preds = [preds_value for i in range(predict_len)]
        model_include = False

    elif str(method).lower() == 'es':
        ts_model = ESModel.ESModel(p_data, param=method_param).fit()
    elif str(method).lower() == 'holt-winter':
        ts_model = HoltWinterModel.HoltWinterModel(p_data, param=method_param["param"]).fit()
    elif str(method).lower() == 'holt':
        ts_model = HoltModel.HoltModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'ar':
        params = method_param['param']
        params_fit = method_param['param_fit']
        ts_model = ARModel.ARModel(p_data, param=params, param_fit=params_fit).fit()
    elif str(method).lower() == 'ma':
        ts_model = MAModel.MAModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'arx':
        ts_model = ARXModel.ARXModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
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
        ts_model = SESModel.SESModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'croston':
        ts_model = CrostonModel.CrostonModel(p_data, param=method_param["param"]).fit()
    elif str(method).lower() == 'crostontsb':
        ts_model = CrostonTSBModel.CrostonTSBModel(p_data, param=method_param["param"]).fit()
    elif str(method).lower() == 'stl':
        ts_model = STLModel.STLModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'theta':
        ts_model = ThetaModel.ThetaModels(p_data, param=method_param["param"],
                                          param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'stlf':
        ts_model = STLForecastModel.STLFModel(p_data, param=method_param["param"],
                                              param_fit=method_param["param_fit"]).fit()
    else:
        ts_model = None
        model_include = False

    result_df = pd.DataFrame()

    if model_include == True:
        preds = ts_model.forecast(predict_len)
        dict_month = {'datetime': preds.index, 'y': preds.values}
        df_month = pd.DataFrame(dict_month)
        if str(method).lower() == 'croston' or str(method).lower() == 'crostontsb':
            result_df['y_pred'] = preds['forecast']
        else:
            result_df['y_pred'] = df_month['y']
    else:
        result_df['y_pred'] = preds
    cur_date_list = list(datetime.datetime.strftime(i, "%Y%m%d") for i in index)
    result_df['dt'] = [i for i in cur_date_list]
    result_df['time_type'] = time_type
    result_df['pred_time'] = forecast_start_date

    data_result = predict_result_handle(result_df, key_value, key_cols, mode_type, save_table_cols)  # 对结果进行处理
    return data_result


def data_process(df, param):
    dt = param['time_col']
    y = param['col_qty']
    key_cols = param['key_cols']

    df[dt] = df[dt].apply(lambda x: pd.to_datetime(x))
    ts = pd.DataFrame(pd.date_range(start=df.dt.min(), end=df.dt.max()), columns=[dt])
    ts = ts.merge(df, on=dt, how='left')
    for i in key_cols:
        ts.loc[:, i] = df.loc[0, i]

    ts_null = ts[ts.isnull().values]
    ts_null.index = range(len(ts_null))

    for i in range(len(ts_null)):
        cur_date = ts_null.loc[i, dt]
        start = pd.to_datetime(cur_date) - pd.Timedelta(days=7)
        end = pd.to_datetime(cur_date) + pd.Timedelta(days=7)
        temp = pd.DataFrame(pd.date_range(start, end), columns=['dt'])
        temp = temp.merge(df, on=dt, how='left')
        y_ = temp.y.mean()
        ts_null.loc[i, y] = y_

    ts2 = pd.concat([df, ts_null])
    data = ts2.sort_values(by=dt, ascending=True)  # 进行排序
    data[dt] = data[dt].apply(lambda x: datetime.datetime.strftime(x, "%Y%m%d"))
    return data
