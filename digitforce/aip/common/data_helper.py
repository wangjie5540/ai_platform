# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    参数操作 ps:供应链场景应用
"""
import traceback

import pandas as pd
import datetime
from pyspark.sql import Row
import numpy as np


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
    # logger_info = get_logger()#日志
    try:
        param=dict_key_lower(param)
        default_conf=dict_key_lower(default_conf)
        for key,value in default_conf.items():
            if isinstance(value,set):
                param[key].update(value)
            if key not in param.keys():
                param[key]=value
        # logger_info.info('update_param_default success')
    except Exception as e:
        param={}
        # logger_info.info(traceback.format_exc())
    return param


def row_transform_to_dataFrame(data):
    """
    row类型转化为dataFrame
    :param data: 原始数据
    :return: 处理好的数据
    """
    if isinstance(data, pd.DataFrame):#pandas版本传入DataFrame类型
        data_tmp = data
    else:#spark版本为row类型
        row_list=list()
        for row in data:
            row_list.append(row.asDict())
        data_tmp=pd.DataFrame(row_list)
    return data_tmp


def dataFrame_transform_to_row(result_df, data_type='pd'):
    """
    row类型转化为dataFrame
    :param data: 原始数据
    :return: 处理好的数据
    """
    if data_type == 'sp':#spark版本为row类型
        resultRow = Row(*result_df.columns)
        data_result = []
        for r in result_df.values:
            data_result.append(resultRow(*r))
        return data_result
    else:
        return result_df


def sales_continue(value, edate, col_qty, col_time, col_key, col_wm='', date_type='day', data_type='pd'):
    """
    need sales continue
    """
    df = row_transform_to_dataFrame(value)
    c_columns = df.columns.tolist()
    c_columns.remove(col_qty)
    sdate = df[col_time].min()
    sr = pd.Series(index=pd.date_range(sdate, edate), data=np.nan)
    st = pd.Series(index=df[col_time].astype('datetime64[ns]'), data=df[col_qty].tolist())
    sr.loc[sr.index.intersection(st.index)] = st.loc[st.index.intersection(sr.index)]
    df.drop(col_qty, axis=1, inplace=True)
    dr = pd.DataFrame(sr).reset_index()
    dr.columns = [col_time, col_qty]
    if date_type == 'day':
        dr[col_time] = dr[col_time].apply(lambda x: x.strftime("%Y%m%d"))
        dr[col_qty].fillna(0, inplace=True)
    elif date_type == 'week':
        dr['week_dt'] = dr[col_time].apply(lambda x: (x - datetime.timedelta(days=x.weekday())).strftime("%Y%m%d"))
        dr[col_qty].fillna(0, inplace=True)
        dr = dr.groupby('week_dt').agg({col_qty: sum}).reset_index().rename(columns={'week_dt': col_time})

    elif date_type == 'month':
        dr['month_dt'] = dr[col_time].apply(
            lambda x: (datetime.date(year=x.year, month=x.month, day=1)).strftime("%Y%m%d"))
        dr[col_qty].fillna(0, inplace=True)
        dr = dr.groupby('month_dt').agg({col_qty: sum}).reset_index().rename(columns={'month_dt': col_time})
    else:
        pass
    df[col_time] = df[col_time].astype(str)
    df = pd.merge(dr, df, on=col_time, how='left')

    # 填充其他
    for column in col_key:
        df[column].fillna(method='ffill', inplace=True)
        df[column].fillna(method='bfill', inplace=True)
    if date_type == 'week':
        df[col_wm] = df[[col_time, col_wm]].apply(lambda x: pd.to_datetime(x[0]).weekofyear if pd.isna(x[1]) else x[1],
                                                  axis=1)
    elif data_type == 'month':
        df[col_wm] = df[[col_time, col_wm]].apply(lambda x: pd.to_datetime(x[0]).month if pd.isna(x[1]) else x[1],
                                                  axis=1)
    else:
        pass
    result_df = dataFrame_transform_to_row(df, data_type)
    return result_df


def compute_year_on_year_ratio(current_value, last_value):
    """计算系数"""
    # 基础倍数
    ratio_upper = 5
    ratio_lower = 0.5

    if pd.isna(current_value) or pd.isna(last_value):
        return 1
    elif current_value == 0.0:
        "整个品类下架"
        return 0
    else:
        inc_ratio = max(ratio_lower, min(ratio_upper, current_value / max(1.0, last_value)))
        return inc_ratio


def days(i):
    """返回天的时间戳"""
    return i * 86400


def tuple_self(list_num):
    """
    自定义tuple用于sql where in
    :param list_num: 仅限 str 类型
    :return:('1003','1004')
    """
    if len(list_num) == 1:
        return "('"+list_num[0]+"')"
    else:
        return tuple(list_num)


def key_process(x, key_cols):
    """
    根据key_cols生成key值
    :param x: value值
    :param key_cols: key的列表
    :return:key值的元数组
    """
    return tuple([x[key] for key in key_cols])


def date_filter_condition(sdata, edata, col='dt'):
    """
    按字段过滤,
    col:列名
    sdata:开始数据
    edata:结束数据
    """
    if sdata == '' and edata != '':
        data_filter = " {0}<={1}".format(col, edata)
    elif sdata != '' and edata == '':
        data_filter = "{0}>={1}".format(sdata)
    elif sdata != '' and edata != '':
        data_filter = "{0}>={1} and {0}<={2}".format(col, sdata, edata)
    else:
        data_filter = '1=1'
    return data_filter


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


