# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：回测spark版本
"""

import os
import sys
file_path=os.path.abspath(os.path.join(os.path.dirname(__file__),'../'))
sys.path.append(file_path)#解决不同位置调用依赖包路径问题
from digitforce.aip.sof.common.date_helper import date_add_str
from src.time_series.sp.predict_sp import method_called_predict_sp
from digitforce.aip.sof.common.data_helper import update_param_default
from digitforce.aip.sof.common.spark import spark_init
from src.time_series.sp.data_prepare import *
from digitforce.aip.sof.common.save_data import write_to_hive
from src.time_series.sp.predict_sp import get_default_conf

def method_called_back_sp(data,key_cols,apply_model_index,param,forcast_start_date,predict_len,step_len,assist_param):
    """
    模型回测
    :param data: 样本
    :param key_cols: FlatMap使用key
    :param apply_model_index: 模型在key_cols中的位置
    :param param: 参数集合
    :param forcast_start_date: 预测开始日期
    :param predict_len: 预测时长
    :param step_len: 回测时每次预测步长
    :param assist_param: 一些辅助函数
    :return: 回测结果
    """
    predict_sum=0
    time_type=assist_param['time_type']#day/week/month
    result_data=None
    if predict_len<=0:
        return result_data
    if step_len<=0:
        step_len=1
    for i in range(predict_len):
        if i!=0:
            forcast_start_date=date_add_str(forcast_start_date,step_len,time_type)
        predict_sum+=step_len
        if predict_sum>predict_len:
            tmp_len=predict_len+step_len-predict_sum
            result_tmp=method_called_predict_sp(data,key_cols,apply_model_index,param,forcast_start_date,tmp_len)
        else:
            result_tmp=method_called_predict_sp(data,key_cols,apply_model_index,param,forcast_start_date,step_len)
        if i==0:
            result_data=result_tmp
        else:
            result_data=result_data.union(result_tmp)#合并结果
        if predict_sum>predict_len:
            break
    return result_data

def back_test_sp(param,spark):
    """
    时序模型运行
    :param param: 参数
    :param spark: spark
    :return:
    """
    status=True
    logger_info=get_logger()
    if 'purpose' not in param.keys() or 'predict_len' not in param.keys():
        logger_info.info('problem:purpose or predict_len')
        return False
    if param['purpose']!='back_test':
        logger_info.info('problem:purpose is not predict')
        return False
    if param['predict_len']<0 or param['predict_len']=='':
        logger_info.info('problem:predict_len is "" or predict_len<0')
        return False
    default_conf=get_default_conf()
    param=update_param_default(param,default_conf)
    logger_info.info("time_series_operation:")
    logger_info.info(str(param))
    mode_type=param['mode_type']
    spark_inner=0
    if str(mode_type).lower()=='sp' and spark==None:
        try:
            add_file='time_series.zip'
            spark=spark_init(add_file)
            logger_info.info('spark 启动成功')
        except Exception as e:
            logger_info.info(traceback.format_exc())
            status=False
        spark_inner=1
    key_cols=param['key_cols']
    apply_model_index=param['apply_model_index']
    forcast_start_date=param['forcast_start_date']
    predict_len=param['predict_len']
    step_len=param['step_len']
    result_processing_param=param['result_processing_param']

    data_sample=data_prepare(spark,param)#样本选择

    try:
        data_pred=method_called_back_sp(data_sample,key_cols, apply_model_index,param,forcast_start_date,predict_len,step_len,param)
        logger_info.info("method called 成功")
    except Exception as e:
        data_pred=None
        status=False
        logger_info.info(traceback.format_exc())

    try:
        partition=result_processing_param['partition']
        table_name=result_processing_param['table_name']
        mode_type=result_processing_param['mode_type']
        write_to_hive(spark,data_pred,partition,table_name,mode_type)#结果保存
    except Exception as e:
        status=False
        logger_info.error(traceback.format_exc())

    if spark_inner==1:#如果当前接口启动的spark，那么要停止
        spark.stop()
        logger_info.info("spark stop")
    return status