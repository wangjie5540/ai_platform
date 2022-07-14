# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    参数操作 ps:供应链场景应用
"""
import traceback
import pandas as pd
from pyspark.sql.types import Row

from forecast.common.log import get_logger
logger_info=get_logger()#日志

def dict_key_lower(param):
    """
    dict的key都变成小写
    :param param: 参数集合：dict
    :return: 转化后的param
    """
    param_new={}
    for key,value in param.items():
        key=str(key).lower()
        if isinstance(value,dict):
            dict_key_lower(value)
        else:
            pass
        param_new[key]=value
    return param_new

def update_param_default(param,default_conf):
    """
    接口传过来的参数和默认参数融合
    :param param: 调用接口传过来的参数
    :param default_conf:默认参数
    :return: 参数集
    """
    try:
        param=dict_key_lower(param)
        default_conf=dict_key_lower(default_conf)
        for key,value in default_conf.items():
            if isinstance(value,set):
                param[key].update(value)
            if key not in param.keys():
                param[key]=value
        logger_info.info('update_param_default success')
    except Exception as e:
        param={}
        logger_info.info(traceback.format_exc())
    return param

def row_transform_to_dataFrame(data):
    """
    row类型转化为dataFrame
    :param data: 原始数据
    :return: 处理好的数据
    """
    if isinstance(data,pd.DataFrame):#pandas版本传入DataFrame类型
        data_tmp=data
    else:#spark版本为row类型
        row_list=list()
        for row in data:
            row_list.append(row.asDict())
        data_tmp=pd.DataFrame(row_list)
    return data_tmp

def predict_result_handle(result_df,key_value,key_cols,mode_type,save_table_cols):
    """
    对预测结果进行处理
    :param result_df:结果表
    :param key_value:key值
    :param key_cols:key列表
    :param mode_type:运行方式
    :param mode_type:运行方式
    :return:
    """
    # key的类型为str
    if isinstance(key_value, str):
        key_value=key_value.split("|")  # 暂定"|"为拼接符
        for i in range(len(key_cols)):
            result_df[key_cols[i]]=key_value[i]
    else:  #key的类型为元组
        for i in range(len(key_cols)):
            result_df[key_cols[i]] = key_value[i]
    if mode_type=='sp':
        resultRow=Row(*result_df.columns)
        data_result=[]
        for r in result_df.values:
            data_result.append(resultRow(*r))
    else:
        data_result=result_df
    data_result=data_result[save_table_cols]
    return data_result
