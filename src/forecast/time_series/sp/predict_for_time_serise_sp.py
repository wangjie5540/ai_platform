# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：预测spark版本
"""
from dateutil.relativedelta import relativedelta
from forecast.common.config import get_config
from forecast.time_series.sp.model_predict import *

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
    conf_default = get_config(time_series_operation, None)
    method_param_all = get_config(time_series, None)  # 模型参数
    conf_default['method_param_all'] = method_param_all
    sales_data_dict = get_config(sales_data_file, 'data')
    conf_default.update(sales_data_dict)
    return conf_default

def key_process(x,key_cols):
    """
    根据key_cols生成key值
    :param x: value值
    :param key_cols: key的列表
    :return:key值的元数组
    """
    return tuple([x[key] for key in key_cols])

def method_called_predict_sp(param,spark_df):
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

    # prepare_data_table = param['prepare_data_table']
    print(key_cols, apply_model_index, forecast_start_date, predict_len, col_qty)
    if predict_len<=0:
        return
    # sqls = '''
    # select * from {0}
    # '''.format(prepare_data_table)
    # spark_df = spark.sql(sqls)
    data_result=spark_df.rdd.map(lambda g:(key_process(g,key_cols),g)).groupByKey(). \
        flatMap(lambda x:model_predict(x[0],x[1],param,key_cols,param,forecast_start_date, predict_len,'sp')).filter(lambda h: h is not None).toDF()

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
    #s数据准备是否成功
    prepare_data = data_prepared_for_model(spark,param)
    output_table = param['output_table']
    partitions = param['partitions']
    preds = method_called_predict_sp(param,prepare_data)
    save_table(spark,preds,output_table,partition=partitions)
    status = "TEST SUCCESS"

    return status

