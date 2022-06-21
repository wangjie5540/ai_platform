# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    各种统计模型
"""
from statistic_model.model.BootStrapModel import BootStrapModel
from common.data_helper import *

def model_predict(key_value,data,method,key_cols,param,forcast_start_date,predict_len,mode_type):
    """
    所有的时序模型,可以实现pipeline和spark级别并行
    :param key_value: key值
    :param data:样本
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
    save_table_cols=param['save_table_cols']

    try:
        method_param=method_param_all['bootstrap']
    except:
        method_param={}
    model_include=True
    data_tmp=row_transform_to_dataFrame(data)
    data_tmp=data_tmp[data_tmp[time_col]<forcast_start_date]#日期小于预测日期
    data_tmp=data_tmp.sort_values(by=time_col,ascending=True)#进行排序
    if str(method).lower()=='bootstrap':
        data_tmp=data_tmp['y']
        sta_model=BootStrapModel(data_tmp,param=method_param)
    else:
        sta_model=None
        model_include=False
    if model_include==True:
        sta_model.fit()
        preds =sta_model.forcast(predict_len)
        result_df = pd.DataFrame()
        result_df['pred_time'] = [i for i in range(1, predict_len + 1)]
        result_df['y_pred'] = preds
        result_df['time_type'] = time_type
        data_result=predict_result_handle(result_df,key_value,key_cols,mode_type,save_table_cols)#对结果进行处理
    else:
        data_result=[]
    return data_result