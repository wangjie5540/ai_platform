# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    价格弹性模型的数主文件
"""

import os
import sys

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(file_path)  # 解决不同位置调用依赖包路径问题
from common.config import get_config
from zipfile import ZipFile
import shutil
from common.data_helper import update_param_default
from common.spark import spark_init
from time_series.sp.data_prepare import *
from time_series.model.time_series_predict import model_predict
from common.save_data import write_to_hive
from price_elasticity import PriceElasticity
from data_prepare import *


def get_default_conf():
    """
    获取时序预测所需的默认参数
    :return: 默认参数
    """
    file_tmp = "/price_elasticity/config/"
    # todo:
    pe_params = file_tmp + 'param.toml'
    time_series = file_tmp + r'model.toml'
    sales_data_file = file_tmp + 'sales_data.toml'
    if os.path.exists(file_path):  # 如果压缩文件存在，是为了兼顾spark_submit形式
        try:
            dst_dir = os.getcwd() + '/zip_tmp'
            zo = ZipFile(file_path, 'r')
            if os.path.exists(dst_dir):
                shutil.rmtree(dst_dir)
            os.mkdir(dst_dir)
            for file in zo.namelist():
                zo.extract(file, dst_dir)
            # time_series_operation=dst_dir+time_series_operation#解压后的地址
            # time_series=dst_dir+time_series  #解压后的地址
            # sales_data_file=dst_dir+sales_data_file
        except:
            time_series_operation = file_path + pe_params  # 解压后的地址
            time_series = file_path + time_series  # 解压后的地址
            sales_data_file = file_path + sales_data_file

    conf_default = get_config(time_series_operation, 'default')
    method_param_all = get_config(time_series)  # 模型参数
    conf_default['method_param_all'] = method_param_all
    sales_data_dict = get_config(sales_data_file, 'data')
    conf_default.update(sales_data_dict)

    try:
        shutil.rmtree(dst_dir)
        zo.close()
    except:
        pass
    return conf_default


def key_process(x, key_cols):
    """
    根据key_cols生成key值
    :param x: value值
    :param key_cols: key的列表
    :return:key值的元数组
    """
    return tuple([x[key] for key in key_cols])


def method_called_train_sp(data, key_cols, apply_model_index, param, hdfs_path, clearance_len):
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
    model = PriceElasticity()  # todo: 这个模型放在什么地方

    if clearance_len <= 0:
        return
    sparkdf_result = data.rdd.map(lambda g: (key_process(g, key_cols), g)).groupByKey(). \
        flatMap(lambda x: model(x[0], x[1], x[0][apply_model_index], param, hdfs_path, clearance_len, 'sp')).toDF()

    return sparkdf_result


def pe_train_model(spark, param):
    """
    时序模型运行
    :param param: 参数
    :param spark: spark
    :return: 返回的是模型的一些参数
    """
    # todo: 清仓的时间段来自于哪里？（参数还是mysql）
    status = True
    spark_inner = 0
    logger_info = get_config()

    default_conf = get_default_conf()
    param = update_param_default(param, default_conf)
    logger_info.info("price elasticity:")
    logger_info.info(str(param))
    mode_type = param['mode_type']  # 选用那种模型log-log 或者是简单的回归模型
    # 所需参数
    key_cols = param['key_cols']
    apply_model_index = param['apply_model_index']
    forcast_start_date = param['forcast_start_date']
    predict_len = param['predict_len']
    result_processing_param = param['result_processing_param']

    data_sample = data_prepare(spark, param)  # 样本选择

    result_coefficient = method_called_train_sp()  # todo:模型的参数设定

    try:
        partition = result_processing_param['partition']
        table_name = result_processing_param['table_name']
        mode_type = result_processing_param['mode_type']
        write_to_hive(spark, result_coefficient, partition, table_name, mode_type)  # 结果保存
        logger_info.info('result_processing_sp 成功')
    except Exception as e:
        status = False
        logger_info.error(traceback.format_exc())

    if spark_inner == 1:  # 如果当前接口启动的spark，那么要停止
        spark.stop()
        logger_info.info("spark stop")
    return status
