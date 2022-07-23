# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：预测spark版本
"""
import os
import sys
file_path=os.path.abspath(os.path.join(os.path.dirname(__file__),'../'))
sys.path.append(file_path)#解决不同位置调用依赖包路径问题
from common.config import get_config
from zipfile import ZipFile
import shutil
from common.data_helper import update_param_default
from common.spark import spark_init
from time_series.sp.data_prepare import *
from time_series.model.time_series_predict import model_predict
from common.save_data import write_to_hive

def get_default_conf():
    """
    获取时序预测所需的默认参数
    :return: 默认参数
    """
    file_tmp="/time_series/config/"
    time_series_operation=file_tmp+'operation.toml'
    time_series=file_tmp+r'model.toml'
    sales_data_file=file_tmp+'sales_data.toml'
    if os.path.exists(file_path):#如果压缩文件存在，是为了兼顾spark_submit形式
        try:
            dst_dir = os.getcwd() + '/zip_tmp'
            zo = ZipFile(file_path, 'r')
            if os.path.exists(dst_dir):
                shutil.rmtree(dst_dir)
            os.mkdir(dst_dir)
            for file in zo.namelist():
                zo.extract(file,dst_dir)
            time_series_operation=dst_dir+time_series_operation#解压后的地址
            time_series=dst_dir+time_series  #解压后的地址
            sales_data_file=dst_dir+sales_data_file
        except:
            time_series_operation=file_path+time_series_operation#解压后的地址
            time_series=file_path+time_series  # 解压后的地址
            sales_data_file=file_path+sales_data_file

    conf_default=get_config(time_series_operation,'default')
    method_param_all=get_config(time_series)#模型参数
    conf_default['method_param_all']=method_param_all
    sales_data_dict=get_config(sales_data_file,'data')
    conf_default.update(sales_data_dict)

    try:
        shutil.rmtree(dst_dir)
        zo.close()
    except:
        pass
    return conf_default

def key_process(x,key_cols):
    """
    根据key_cols生成key值
    :param x: value值
    :param key_cols: key的列表
    :return:key值的元数组
    """
    return tuple([x[key] for key in key_cols])

def method_called_predict_sp(data,key_cols,apply_model_index,param,forcast_start_date,predict_len):
    """
    模型调用
    :param data:样本
    :param key_cols:FlatMap使用key
    :param apply_model_index: 模型在key_cols中的位置
    :param param: 参数集合
    :param forcast_start_date: 预测开始日期
    :param predict_len: 预测时长
    :return: 预测后的结果
    """
    if predict_len<=0:
        return
    data_result=data.rdd.map(lambda g:(key_process(g,key_cols),g)).groupByKey(). \
        flatMap(lambda x:model_predict(x[0],x[1],x[0][apply_model_index],key_cols,param,forcast_start_date, predict_len,'sp')).filter(
        lambda h: h is not None).toDF()
    return data_result

def predict_sp(param,spark):
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
    if param['purpose'] != 'predict':
        logger_info.info('problem:purpose is not predict')
        return False
    if param['predict_len'] < 0 or param['predict_len'] == '':
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
            status=False
            logger_info.info(traceback.format_exc())
        spark_inner=1

    #所需参数
    key_cols=param['key_cols']
    apply_model_index=param['apply_model_index']
    forcast_start_date=param['forcast_start_date']
    predict_len=param['predict_len']
    result_processing_param=param['result_processing_param']

    data_sample=data_prepare(spark,param)#样本选择

    try:
        data_pred=method_called_predict_sp(data_sample,key_cols, apply_model_index,param,forcast_start_date,predict_len)
        logger_info.info("time_series predict 成功")
    except Exception as e:
        data_pred=None
        status=False
        logger_info.info(traceback.format_exc())

    try:
        partition=result_processing_param['partition']
        table_name=result_processing_param['table_name']
        mode_type=result_processing_param['mode_type']
        write_to_hive(spark,data_pred, partition, table_name, mode_type)#结果保存
        logger_info.info('result_processing_sp 成功')
    except Exception as e:
        status=False
        logger_info.error(traceback.format_exc())

    if spark_inner==1:#如果当前接口启动的spark，那么要停止
        spark.stop()
        logger_info.info("spark stop")
    return status
