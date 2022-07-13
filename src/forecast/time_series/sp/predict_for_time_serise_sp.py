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
    try:
        method_param=method_param_all[method]
    except:
        method_param={}
    model_include=True
    data = row_transform_to_dataFrame(data)
    data_tmp=data[data[time_col]<forcast_start_date]#日期小于预测日期
    data_tmp=data_tmp.sort_values(by=time_col,ascending=True)#进行排序

    p_data = data_tmp[['th_y', 'dt']].set_index('dt')
    p_data['th_y'] = p_data['th_y'].astype(float)
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>pdata", p_data.head(10))
    print("metod:%s,time_col:%s,time_type:%s"%(method_param_all,time_col,time_type))

    if str(method).lower() == 'es':
        # data_tmp=data_tmp['y']#只取y列
        ts_model = ESModel.ESModel(p_data, param=method_param)
    elif str(method).lower() == 'holt-winter':
        if data.shape[0] < 17:  # 数据量太少
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>数据量少，进入dms模型", data.shape[0])
            data_tmp = p_data['th_y']  # 只取y列
            ts_model = DmsModel.DmsModel(data_tmp, param=method_param)
        else:
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>进入holtwiner",
                  data.shape[0])
            ts_model = HoltWinterModel.HoltWinterModel(p_data, param=method_param["param"])

    elif str(method).lower() == 'holt':
        ts_model = HoltModel.HoltModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"])
    elif str(method).lower() == 'ar':
        params = method_param['param']
        params_fit = method_param['param_fit']
        ts_model = ARModel.ARModel(p_data, param=params, param_fit=params_fit)
    elif str(method).lower() == 'ma':
        ts_model = MAModel.MAModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"])
    elif str(method).lower() == 'arx':
        ts_model = ARXModel.ARXModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"])
    elif str(method).lower() == 'arima':
        ts_model = Stats_ARIMAModel.ARIMAModel(p_data, param=method_param["param"],param_fit=method_param["param_fit"])
    elif str(method).lower() == 'arimax':
        ts_model = ARIMAXModel.ARIMAXModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"])
    elif str(method).lower() == 'sarima':
        ts_model = SARIMAModel.SARIMAModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"])
    elif str(method).lower() == 'sarimax':
        ts_model = SARIMAXModel.SARIMAXModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"])
    elif str(method).lower() == 'ses':
        ts_model = SESModel.SESModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"])
    elif str(method).lower() == 'croston':
        ts_model = CrostonModel.CrostonModel(p_data, param=method_param["param"])
    elif str(method).lower() == 'crostontsb':
        ts_model = CrostonTSBModel.CrostonTSBModel(p_data, param=method_param["param"])
    elif str(method).lower() == 'stl':
        ts_model = STLModel.STLModel(p_data, param=method_param["param"], param_fit=method_param["param_fit"])
    elif str(method).lower() == 'theta':
        ts_model = ThetaModel.ThetaModels(p_data, param=method_param["param"], param_fit=method_param["param_fit"])
    elif str(method).lower() == 'stlf':
        ts_model = STLForecastModel.STLFModel(p_data, param=method_param["param"],param_fit=method_param["param_fit"])
    else:
        ts_model = None
        model_include = False

    if model_include==True:
        ts_model.fit()
        preds=ts_model.forcast(predict_len)
        result_df=pd.DataFrame()
        dict_month = {'datetime': preds.index, 'y': preds.values}
        df_month = pd.DataFrame(dict_month)

        result_df['pred_time']=[i for i in range(1,predict_len+1)]
        if str(method).lower() == 'croston' or str(method).lower() == 'crostontsb':
            result_df['y_pred'] = preds['forecast']
        else:
            result_df['y_pred'] = df_month['y']
        result_df['time_type']=time_type
        data_result=predict_result_handle(result_df,key_value,key_cols,mode_type,save_table_cols)#对结果进行处理
    else:
        data_result=[]
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


def method_called_predict_sp(data,key_cols,apply_model_index,param,forecast_start_date,predict_len):
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
    if predict_len<=0:
        return
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>data",data.show(10))
    data_result=data.rdd.map(lambda g:(key_process(g,key_cols),g)).groupByKey(). \
        flatMap(lambda x:model_predict(x[0],x[1],x[0][apply_model_index],key_cols,param,forecast_start_date, predict_len,'sp')).filter(lambda h: h is not None).toDF()
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
    status=True
    # logger_info=get_logger()
    if 'purpose' not in param.keys() or 'predict_len' not in param.keys():
        # logger_info.info('problem:purpose or predict_len')
        print('problem:purpose or predict_len')
        return False
    if param['purpose'] != 'predict':
        # logger_info.info('problem:purpose is not predict')
        print('problem:purpose is not predict')
        return False
    if param['predict_len'] < 0 or param['predict_len'] == '':
        # logger_info.info('problem:predict_len is "" or predict_len<0')
        print('problem:predict_len is "" or predict_len<0')
        return False
    default_conf=get_default_conf()
    # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>default_conf",default_conf)
    # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>param before",param)
    param=update_param_default(param,default_conf)
    # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>param after",param)
    # logger_info.info("time_series_operation:")
    # logger_info.info(str(param))
    mode_type=param['mode_type']
    spark_inner=0
    if str(mode_type).lower()=='sp' and spark==None:
        try:
            add_file='time_series.zip'
            spark=spark_init(add_file)
            # logger_info.info('spark 启动成功')
        except Exception as e:
            status=False
            # logger_info.info(traceback.format_exc())
        spark_inner=1

    #所需参数
    key_cols = param['key_cols']
    apply_model_index = param['apply_model_index']
    forecast_start_date = param['forecast_start_date']
    predict_len = param['predict_len']
    result_processing_param = param['result_processing_param']
    save_table_cols = param['default']['save_table_cols']

    data_sample = data_prepared_for_model(spark,param)#样本选择
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>..data_sample",data_sample.show(10))

    try:
        data_pred=method_called_predict_sp(data_sample,key_cols, apply_model_index,param,forecast_start_date,predict_len)
        # logger_info.info("time_series predict 成功")
        # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>data_pred",data_pred)
    except Exception as e:
        # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>predict_sp",e)
        data_pred=None
        status=False
        # logger_info.info(traceback.format_exc())


    try:
        partition=result_processing_param['partition']
        table_name=result_processing_param['table_name']
        mode_type=result_processing_param['mode_type']

        write_to_hive(spark,data_pred, partition, table_name, mode_type)#结果保存
        # logger_info.info('result_processing_sp 成功')
    except Exception as e:
        status=False
        # logger_info.error(traceback.format_exc())

    if spark_inner==1:#如果当前接口启动的spark，那么要停止
        spark.stop()
        # logger_info.info("spark stop")
    return status

