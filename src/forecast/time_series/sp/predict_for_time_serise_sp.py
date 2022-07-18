# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：预测spark版本
"""
import argparse
import os
import sys
import traceback
import datetime

import numpy as np
import pandas as pd
# file_path=os.path.abspath(os.path.join(os.path.dirname(__file__),'../'))
# sys.path.append(file_path)#解决不同位置调用依赖包路径问题
from dateutil.relativedelta import relativedelta
from forecast.common.config import get_config
from zipfile import ZipFile
import shutil
from forecast.common.data_helper import *
from forecast.common.spark import spark_init
from forecast.time_series.sp.data_prepare_for_time_series_sp import *
from forecast.time_series.model import ARModel,ARXModel,ARIMAXModel,Stats_ARIMAModel,ThetaModel,SARIMAXModel,MAModel,SARIMAModel,SESModel,STLModel,ESModel,CrostonModel,CrostonTSBModel,HoltModel,HoltWinterModel,STLForecastModel,DmsModel
from forecast.common.save_data import write_to_hive
from forecast.common.common_helper import *
# from forecast.common.log import get_logger


'''
整体思路：
1，获得输入参数，通过参数解析选择模型
2，数据准备
3，模型预测
4，效果评估
5，结果存储
'''

def model_predict(key_value,data,method,key_cols,param,forcast_start_date,predict_len,mode_type):
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
    data = row_transform_to_dataFrame(data)
    data[time_col] = data[time_col].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))
    method = data[apply_model].values[0]
    method_param = method_param_all[method]

    data_tmp=data[data[time_col]<forcast_start_date]#日期小于预测日期
    data_tmp=data_tmp.sort_values(by=time_col,ascending=True)#进行排序

    p_data = data_tmp[[y,time_col]].set_index(time_col)
    p_data[y] = p_data[y].astype(float)
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>pdata", p_data.head(10))
    print("metod:%s,time_col:%s,time_type:%s"%(method_param_all,time_col,time_type))

    if data.shape[0]<17:
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
            result_df['pred_time']=preds.index
    else:
        result_df['y_pred'] = preds
    result_df['pred_time'] = [i for i in range(1, predict_len + 1)]
    result_df['time_type']=time_type

    data_result=predict_result_handle(result_df,key_value,key_cols,mode_type,save_table_cols)#对结果进行处理
    return data_result

#获取参数
def get_default_conf():
    """
    获取时序预测所需的默认参数
    :return: 默认参数
    """
    file_tmp="forecast/time_series/config/"
    time_series_operation=file_tmp+'operation.toml'
    time_series=file_tmp+r'model.toml'
    sales_data_file=file_tmp+'sales_data.toml'
    # if os.path.exists(file_tmp):#如果压缩文件存在，是为了兼顾spark_submit形式
    #     try:
    #         dst_dir = os.getcwd() + '/zip_tmp'
    #         zo = ZipFile(file_tmp, 'r')
    #         if os.path.exists(dst_dir):
    #             shutil.rmtree(dst_dir)
    #         os.mkdir(dst_dir)
    #         for file in zo.namelist():
    #             zo.extract(file,dst_dir)
    #         time_series_operation=dst_dir+time_series_operation#解压后的地址
    #         time_series=dst_dir+time_series  #解压后的地址
    #         sales_data_file=dst_dir+sales_data_file
    #     except:
    #         time_series_operation=file_tmp+time_series_operation#解压后的地址
    #         time_series=file_tmp+time_series  # 解压后的地址
    #         sales_data_file=file_tmp+sales_data_file

    conf_default = get_config(time_series_operation, None)

    method_param_all = get_config(time_series, None)  # 模型参数

    conf_default['method_param_all'] = method_param_all

    sales_data_dict = get_config(sales_data_file, 'data')

    conf_default.update(sales_data_dict)


    # try:
    #     shutil.rmtree(dst_dir)
    #     zo.close()
    # except:
    #     pass
    return conf_default

def key_process(x,key_cols):
    """
    根据key_cols生成key值
    :param x: value值
    :param key_cols: key的列表
    :return:key值的元数组
    """
    return tuple([x[key] for key in key_cols])


def method_called_predict_sp(spark,param):
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
    col_qty = param['col_qty']

    prepare_data_table = param['prepare_data_table']
    print(key_cols, apply_model_index, forecast_start_date, predict_len, col_qty)
    if predict_len<=0:
        return
    sqls = '''
    select * from {0}
    '''.format(prepare_data_table)
    spark_df = spark.sql(sqls)
    data_result=spark_df.rdd.map(lambda g:(key_process(g,key_cols),g)).groupByKey(). \
        flatMap(lambda x:model_predict(x[0],x[1],param,key_cols,param,forecast_start_date, predict_len,'sp')).filter(lambda h: h is not None).toDF()
    # save_table(spark,data_result,output_table,partition=partitions)
    return data_result

def date_add_str(date_str,step_len,time_type='day'):
    """
    新的日期=当前日期+step_len*(day/周/月)
    :param date_str:当前日期
    :param step_len:多长时间
    :param time_type:day/week/month
    :return:新的日期
    """
    if time_type=='day':#日
        date_str=datetime.datetime.strptime(date_str,"%Y%m%d")
        date_str_add=date_str+datetime.timedelta(days=+step_len)
        date_str_add=str(date_str_add).replace('-','')[0:8]
    elif time_type=='week':#周
        date_str=datetime.datetime.strptime(date_str, "%Y%m%d")
        date_str_add=date_str + datetime.timedelta(weeks=+step_len)
        date_str_add=str(date_str_add).replace('-','')[0:8]
    else:#月
        date_str=datetime.datetime.strptime(date_str,'%Y%m%d')
        date_str_add=date_str+relativedelta(months=+step_len)
        date_str_add=str(date_str_add).replace('-','')[0:8]
    return date_str_add


def predict_sp(param,spark):
    """
    时序模型运行
    :param param: 参数
    :param spark: spark
    :return:
    """
    #s数据准备是否成功？
    prepare_success = data_prepared_for_model(spark,param)
    status = False
    if prepare_success:
        output_table = param['output_table']
        partitions = param['partitions']
        preds = method_called_predict_sp(spark,param)
        save_table(spark,preds,output_table,partition=partitions)
        status = "SUCCESS"

    return status

