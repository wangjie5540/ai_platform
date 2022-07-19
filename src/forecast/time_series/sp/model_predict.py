# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：预测spark版本
"""

from forecast.common.data_helper import *
from forecast.time_series.sp.data_prepare_for_time_series_sp import *
from forecast.time_series.model import ARModel,ARXModel,ARIMAXModel,Stats_ARIMAModel,ThetaModel,SARIMAXModel,MAModel,SARIMAModel,SESModel,STLModel,ESModel,CrostonModel,CrostonTSBModel,HoltModel,HoltWinterModel,STLForecastModel,DmsModel
from forecast.common.common_helper import *


'''
整体思路：
1，获得输入参数，通过参数解析选择模型
2，数据准备
3，模型预测
4，效果评估
5，结果存储
'''

def model_predict(key_value,data,param,forcast_start_date,predict_len):
    """
    所有的时序模型预测,可以实现pipeline和spark级别并行
    :param key_value: key值
    :param data: 样本
    :param method: 选择方法
    :param key_cols: key值的关键字
    :param param: 参数集合
    :param forcast_start_date: 预测开始日期
    :param predict_len: 预测时长
    :param mode_type: 运行方式
    :return: 预测结果
    """
    method_param_all=param['method_param_all']
    time_col=param['time_col']
    time_type=param['time_type']
    save_table_cols=param['default']['save_table_cols']
    key_cols = param['key_cols']
    mode_type = param['mode_type']
    y = param['col_qty']
    apply_model = param['apply_model']
    model_include=True
    data_temp = row_transform_to_dataFrame(data)

    #天维度连续性检测
    data = data_temp
    if param['time_type'] == 'day':
        data = data_process(data_temp,param)

    temp_dict = {"day": "D", "week": "W-MON", "month": "MS", "season": "QS-OCT", "year": "A"}

    data[time_col] = data[time_col].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
    method = data[apply_model].values[0]
    method_param = method_param_all[method]

    index = pd.date_range(forcast_start_date, periods=predict_len, freq="D")
    # forcast_end_date = datetime.datetime.strftime(max(index), "%Y%m%d")
    data_tmp = data[data[time_col] < forcast_start_date]#日期小于预测日期


    # if param['purpose'] == 'back_test':
    #     if param['time_type'] in temp_dict:
    #         index = pd.date_range(forcast_start_date, periods=predict_len, freq=temp_dict[param['time_type']])
    #         forcast_end_date = datetime.datetime.strftime(max(index),"%Y%m%d")
    #     data_tmp = data[data[time_col]>=forcast_start_date and data[data[time_col]<=forcast_end_date]]#回测数据集


    data_tmp=data_tmp.sort_values(by=time_col,ascending=True)#进行排序

    p_data = data_tmp[[y,time_col]].set_index(time_col)
    p_data[y] = p_data[y].astype(float)


    if data.shape[0]<17:
        preds_value = data[y].mean()
        pred = [preds_value for i in range(predict_len)]
        preds = pd.Series(pred,index)
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
        ts_model = Stats_ARIMAModel.ARIMAModel(p_data, param=method_param["param"],param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'arimax':
        ts_model = ARIMAXModel.ARIMAXModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'sarima':
        ts_model = SARIMAModel.SARIMAModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'sarimax':
        ts_model = SARIMAXModel.SARIMAXModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'ses':
        ts_model = SESModel.SESModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'croston':
        ts_model = CrostonModel.CrostonModel(p_data, param=method_param["param"]).fit()
    elif str(method).lower() == 'crostontsb':
        ts_model = CrostonTSBModel.CrostonTSBModel(p_data, param=method_param["param"]).fit()
    elif str(method).lower() == 'stl':
        ts_model = STLModel.STLModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'theta':
        ts_model = ThetaModel.ThetaModels(p_data, param=method_param["param"], param_fit=method_param["param_fit"]).fit()
    elif str(method).lower() == 'stlf':
        ts_model = STLForecastModel.STLFModel(p_data, param=method_param["param"],param_fit=method_param["param_fit"]).fit()
    else:
        ts_model = None
        model_include = False

    result_df=pd.DataFrame()

    if model_include==True:
        preds=ts_model.forecast(predict_len)
        dict_month = {'datetime': preds.index, 'y': preds.values}
        df_month = pd.DataFrame(dict_month)
        if str(method).lower() == 'croston' or str(method).lower() == 'crostontsb':
            result_df['y_pred'] = preds['forecast']
            result_df['pred_time'] = preds['dt']
        else:
            result_df['y_pred'] = df_month['y']
            result_df['pred_time'] = df_month['datetime']
    else:
        result_df['y_pred'] = preds.values
        result_df['pred_time'] =preds.index
    # result_df['pred_time'] = [i for i in range(1, predict_len + 1)]
    result_df['time_type']=time_type

    # result_df['pred_time'] = result_df['pred_time'].apply(lambda x:datetime.datetime.strftime(x,"%Y%m%d"))
    data_result=predict_result_handle(result_df,key_value,key_cols,mode_type,save_table_cols)#对结果进行处理
    return data_result

def data_process(df,param):
    dt = param['time_col']
    predict_start = param['forecast_start_date']
    y = param['col_qty']
    try:
        df[dt] = df[dt].apply(lambda x: pd.to_datetime(x))
        ts = pd.DataFrame(pd.date_range(start=df.dt.min(), end=df.dt.max()), columns=[dt])
        ts = ts.merge(df, on=dt, how='left')

        last_day = pd.to_datetime(predict_start) - pd.Timedelta(days=1)

        if ts.dt.iloc[-1].dayofweek != last_day.dayofweek:
            diff = (last_day.dayofweek - ts.dt.iloc[-1].dayofweek) % 7
            for d in pd.date_range(end=last_day, periods=diff):
                ts = ts.append({dt: d, 'days_week': d.dayofweek}, ignore_index=True)
        ts.dt = pd.date_range(end=last_day, periods=ts.shape[0])

        ts[dt] = ts[dt].apply(lambda x: pd.to_datetime(x))
        sales_cleaned = pd.DataFrame()
        for d in range(0, 7):
            t = ts.loc[ts.dt.dt.dayofweek == d].sort_values(dt).reset_index(drop=True)
            t['dy'] = t[y].shift(-1)
            t['uy'] = t[y].shift(1)
            t.dy = t.dy.fillna(method='ffill').fillna(method='bfill')
            t.uy = t.uy.fillna(method="ffill").fillna(method="bfill")

            t[y] = t[y].fillna((t.dy + t.uy) / 2)
            t[y].fillna(method="ffill", inplace=True)
            t[y].fillna(method="bfill", inplace=True)
            t[y].fillna(t[y].rolling(7, min_periods=0, center=True).mean(), inplace=True)
            t[y].fillna(0.0, inplace=True)
            sales_cleaned = sales_cleaned.append(t)

        sales_cleaned = sales_cleaned.sort_values(dt)
        sales_cleaned = sales_cleaned.dropna()
        # sales_cleaned[dt] = sales_cleaned[dt].apply(lambda x: datetime.datetime.strftime(x, "%Y%m%d"))

        logger_info.info("数据准备完成！")
    except Exception as e:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>...data prepare",e)
        logger_info.info(traceback.format_exc())

    return sales_cleaned
