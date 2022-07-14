# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    机器学习模型：训练spark版本
"""
import os
import sys
import traceback
file_path=os.path.abspath(os.path.join(os.path.dirname(__file__),'../'))
sys.path.append(file_path)#解决不同位置调用依赖包路径问题
from common.log import get_logger
from ml_model.sp.data_prepare import data_prepare_train
from ml_model.model.ml_train import ml_train
from ml_model.sp.predict_sp import key_process,get_default_conf
from common.data_helper import update_param_default
from common.spark import spark_init

def method_called_train_sp(data,key_cols,apply_model_index,param,hdfs_path,predict_len):
    """
    模型训练
    :param data:样本
    :param key_cols: key的列名
    :param apply_model_index: 模型的index
    :param param: 参数
    :param hdfs_path: 保存hdfs地址
    :param predict_len: 预测时长
    :return:
    """
    if predict_len<=0:
        return
    data.rdd.map(lambda g:(key_process(g,key_cols),g)).groupByKey(). \
        flatMap(lambda x:ml_train(x[0],x[1],x[0][apply_model_index],param,hdfs_path,predict_len,'sp'))

def train_sp(param,spark):
    """
    机器学习模型的预测和回测
    :param param: 参数
    :param spark: spark
    :return:
    """
    status=True
    logger_info = get_logger()
    if 'purpose' not in param.keys() or 'predict_len' not in param.keys():
        logger_info.info('problem:purpose or predict_len')
        return False
    if param['purpose'] != 'train':
        logger_info.info('problem:purpose is not train')
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
        except Exception as e:
            status=False
            logger_info.info(traceback.format_exc())
        spark_inner=1
    key_cols=param['key_cols']
    apply_model_index=param['apply_model_index']
    predict_len=param['predict_len']
    hdfs_path=param['hdfs_path']

    try:
        data_train=data_prepare_train(spark,param)#训练样本
        method_called_train_sp(data_train,key_cols,apply_model_index, param,hdfs_path,predict_len)
    except Exception as e:
        status=False
        logger_info.info(traceback.format_exc())

    if spark_inner==1:#如果当前接口启动的spark，那么要停止
        spark.stop()
        logger_info.info("spark stop")
    return status