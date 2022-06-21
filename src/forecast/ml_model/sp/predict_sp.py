# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    机器学习模型：预测spark版本
"""

import os
import sys
file_path=os.path.abspath(os.path.join(os.path.dirname(__file__),'../'))
sys.path.append(file_path)#解决不同位置调用依赖包路径问题
from zipfile import ZipFile
import shutil
from common.config import get_config
from common.data_helper import update_param_default
from common.spark import spark_init
from ml_model.sp.data_prepare import *
from ml_model.model.ml_predict import ml_predict
from common.save_data import write_to_hive

def get_default_conf():
    """
    获取时序预测所需的默认参数
    :return: 默认参数
    """
    file_tmp="/ml_model/config/"
    ml_model_operation=file_tmp+'operation.toml'
    ml_model=file_tmp+r'model.toml'
    sales_data_file=file_tmp+r'sales_data.toml'
    if os.path.exists(file_path):#如果压缩文件存在，是为了兼顾spark_submit形式
        try:
            dst_dir = os.getcwd()+'/zip_tmp'
            zo = ZipFile(file_path,'r')
            if os.path.exists(dst_dir):
                shutil.rmtree(dst_dir)
            os.mkdir(dst_dir)
            for file in zo.namelist():
                zo.extract(file,dst_dir)
            ml_model_operation=dst_dir + ml_model_operation #解压后的地址
            ml_model=dst_dir+ml_model #解压后的地址
            sales_data_file=dst_dir+sales_data_file
        except:
            ml_model_operation=file_path+ml_model_operation #解压后的地址
            ml_model=file_path+ml_model  # 解压后的地址
            sales_data_file=file_path+sales_data_file

    conf_default=get_config(ml_model_operation,'default')
    method_param_all=get_config(ml_model)#模型参数
    conf_default['method_param_all']=method_param_all
    sales_data_dict=get_config(sales_data_file, 'data')
    conf_default.update(sales_data_dict)

    try:
        shutil.rmtree(dst_dir)
        zo.close()
    except:
        pass
    return conf_default

def key_process(x,key_list):
    """
    根据key_list生成key值
    :param x: value值
    :param key_list: key的列表
    :return:key值的元数组
    """
    return tuple([x[key] for key in key_list])

def method_called_predict_sp(data,key_cols,hdfs_path,param,predict_len):
    """
    模型预测
    :param data: 预测特征
    :param key_cols: key的列
    :param hdfs_path: 保存hdfs地址
    :param param: 参数
    :param predict_len: 预测时长
    :return: 预测值
    """
    if predict_len<=0:
        return
    data_result=data.rdd.map(lambda g:(key_process(g,key_cols),g)).groupByKey(). \
        flatMap(lambda x:ml_predict(x[0],x[1],predict_len,hdfs_path,param,'sp')).filter(
        lambda h: h is not None).toDF()
    return data_result

def predict_sp(param,spark):
    """
    机器学习模型的预测
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
    param=update_param_default(param, default_conf)
    logger_info.info("ml_time_operation:")
    logger_info.info(str(param))
    mode_type=param['mode_type']
    spark_inner=0
    if str(mode_type).lower()=='sp' and spark==None:
        try:
            spark=spark_init()
            logger_info.info('spark 启动成功')
            status=False
        except Exception as e:
            logger_info.info(traceback.format_exc())
        spark_inner=1
    key_cols=param['key_cols']
    predict_len=param['predict_len']
    result_processing_param=param['result_processing_param']
    hdfs_path=param['hdfs_path']

    try:
        data_predict=data_prepare_predict(spark,param)#预测样本
        data_pred=method_called_predict_sp(data_predict,key_cols, hdfs_path,param,predict_len)
        status=False
    except Exception as e:
        data_pred=None
        logger_info.info(traceback.format_exc())
    if data_pred is not None:#预测和回测的结果写表
        try:
            partition =result_processing_param['partition']
            table_name = result_processing_param['table_name']
            mode_type = result_processing_param['mode_type']
            write_to_hive(spark, data_pred, partition, table_name, mode_type)  # 结果保存
            logger_info.info('result_processing_sp 成功')
        except Exception as e:
            logger_info.info(traceback.format_exc())

    if spark_inner==1:#如果当前接口启动的spark，那么要停止
        spark.stop()
        logger_info.info("spark stop")
    return status