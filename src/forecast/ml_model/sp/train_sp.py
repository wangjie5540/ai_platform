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
# from digitforce.aip.common.log import get_logger
from forecast.ml_model.sp.data_prepare import data_prepare_train
from forecast.ml_model.model.ml_train import ml_train
from forecast.ml_model.sp.predict_sp import key_process, get_default_conf
from digitforce.aip.common.data_helper import update_param_default
from forecast.common.spark import spark_init
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
import logging
# from digitforce.aip.common.spark_helper import SparkHelper,forecast_spark_session
import pyspark.sql.functions as psf

# file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
# sys.path.append(file_path)  # 解决不同位置调用依赖包路径问题
logger_info = setup_console_log()
setup_logging(info_log_file="sales_fill_zero.info", error_log_file="", info_log_file_level="INFO")


def method_called_train_sp(data, key_cols, apply_model_index, param, hdfs_path, predict_len):
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
    try:
        back_testing = param['back_testing']
    except:
        back_testing = None
    print("back_testing is ", back_testing)
    if predict_len <= 0:
        return
    xx_test = data.groupBy(key_cols).agg(psf.count(psf.col('dt')).alias('nums'))
    print("sadsad", xx_test.show(100))
    result = data.rdd.map(lambda g: (key_process(g, key_cols), g)).groupByKey(). \
        flatMap(lambda x: ml_train(x[0], x[1], x[0][apply_model_index],
                                   param, hdfs_path, predict_len, 'sp', back_testing)).toDF()
    #     result = data.rdd.groupBy(lambda x:x['shop_id']+'/'+x['apply_model']+'/'+str(x['group_category'])). \
    #         flatMap(lambda x: ml_train(x[0], x[1], x[0][apply_model_index],
    #                                    param, hdfs_path, predict_len, 'sp',back_testing)).toDF()
    print("result sucess")
    # result.show()

    result_df = result.toPandas()


def train_sp(param, spark):
    """
    机器学习模型的预测和回测
    :param param: 参数
    :param spark: spark
    :return:
    """
    status = True
    if 'purpose' not in param.keys() or 'predict_len' not in param.keys():
        logging.info('problem:purpose or predict_len')
        return False
    if param['purpose'] != 'train':
        logging.info('problem:purpose is not train')
        return False
    if param['predict_len'] < 0 or param['predict_len'] == '':
        logging.info('problem:predict_len is "" or predict_len<0')
        return False
    default_conf = get_default_conf()
    param = update_param_default(param, default_conf)
    logging.info("ml_time_operation:")
    logging.info(str(param))
    mode_type = param['mode_type']
    spark_inner = 0
    if str(mode_type).lower() == 'sp' and not spark:
        try:
            logging.info('spark 启动成功')
        except Exception as e:
            status = False
            logging.info(traceback.format_exc())
        spark_inner = 1
    key_cols = param['col_keys']
    apply_model_index = param['apply_model_index']
    predict_len = param['predict_len']
    hdfs_path = param['hdfs_path']


    try:
        data_train = data_prepare_train(spark, param).fillna(0)  # 训练样本
        data_train = data_train.fillna(0)
        data_train = data_train.na.fill("a")
        #         data_train=data_train.filter(data_train['group_category'].isin(group_category_select))

        #         replaceDict = {3:23,13:213}
        #         data_train = data_train.replace(replaceDict,subset=['group_category'])
        data_train.show()
        logging.info("提取数据成功")
        method_called_train_sp(data_train, key_cols, apply_model_index, param, hdfs_path, predict_len)
        logging.info("模型训练成功")
    except Exception as e:
        status = False
        logging.info(traceback.format_exc())

    if spark_inner == 1:  # 如果当前接口启动的spark，那么要停止
        spark.stop()
        logging.info("spark stop")
    return status
