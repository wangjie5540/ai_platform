# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    数据分组：spark版，包括：模型选择+分组
"""
import os
import sys
file_path=os.path.abspath(os.path.join(os.path.dirname(__file__),'../'))
sys.path.append(file_path)#解决不同位置调用依赖包路径问题
from common.config import get_config
from zipfile import ZipFile
import shutil
from common.spark import spark_init
from common.log import get_logger
import traceback
import copy
from data_split.sp.model_selection_sp import model_selection
from data_split.sp.grouping_category import group_category
from common.save_data import write_to_hive

def get_default_conf():
    """
    获取时序预测所需的默认参数
    :return: 默认参数
    """
    file_tmp="/config/"
    operation=file_tmp+'operation.toml'
    data_prepare=file_tmp+'data_prepare.toml'
    method=file_tmp+'method.toml'
    grouping_condition=file_tmp+'group_category.toml'
    if os.path.exists(file_path):#如果压缩文件存在，是为了兼顾spark_submit形式
        try:
            dst_dir=os.getcwd()+'/zip_tmp'
            zo=ZipFile(file_path,'r')
            if os.path.exists(dst_dir):
                shutil.rmtree(dst_dir)
            os.mkdir(dst_dir)
            for file in zo.namelist():
                zo.extract(file,dst_dir)
            operation=dst_dir+operation#解压后的地址
            data_prepare=dst_dir+data_prepare  #解压后的地址
            method=dst_dir+method
            grouping_condition=dst_dir+grouping_condition
        except:
            operation=file_path+operation#解压后的地址
            data_prepare=file_path+data_prepare #解压后的地址
            method=file_path+method
            grouping_condition=file_path+grouping_condition
    conf_default=get_config(operation,'default')
    result_processing_param=get_config(operation,'result_processing_param')#写结果的参数
    conf_default['result_processing_param']=result_processing_param
    data_prepare_dict=get_config(data_prepare)
    data_prepare_dict_basic=data_prepare_dict['basic']
    conf_default.update(data_prepare_dict_basic)
    conf_default['sales_data']=data_prepare_dict['sales_data']
    method_param=get_config(method)#模型参数
    conf_default['method_param']=method_param
    grouping_condition_param=get_config(grouping_condition)
    conf_default['grouping_condition']=grouping_condition_param
    try:
        shutil.rmtree(dst_dir)
        zo.close()
    except:
        pass
    return conf_default

def write_result(spark,data,param):
    """
    模型选择和分组结果写表
    :param spark:
    :param data: 要写入的数据
    :param param: 参数
    :return:
    """
    logger_info=get_logger()
    if data==None:
        logger_info.info("数据为空")
        return
    try:
        master_key_cols=param['master_key_cols']
        result_processing_param=param['result_processing_param']#写入参数
        partition=result_processing_param['partition']
        table_name=result_processing_param['table_name']
        mode_type=result_processing_param['mode_type']
        select_cols=copy.copy(master_key_cols)
        select_cols.extend(['apply_model','group_category'])#模型选择和分组结果
        data=data.select(select_cols)
        write_to_hive(spark,data,partition,table_name,mode_type)#结果保存到hive中
    except Exception as e:
        logger_info.info(traceback.format_exc())


def data_split_sp(param,spark):
    """
    时序模型运行
    :param param: 参数
    :param spark: spark
    :return:
    """
    status=True
    spark_inner=0
    mode_type=param['mode_type']#运行方式
    grouping_condition=param['grouping_condition']
    logger_info=get_logger()

    if str(mode_type).lower()=='sp' and spark==None:
        try:
            spark=spark_init()
            logger_info.info('spark 启动成功')
        except Exception as e:
            status=False
            logger_info.info(traceback.format_exc())
        spark_inner=1

    try:
        model_selection_result=model_selection(spark,param)#第一步：模型选择
        data_split_result=group_category(model_selection_result,grouping_condition)#第二步：分组
    except:
        logger_info.info(traceback.format_exc())
        data_split_result=None
    write_result(spark,data_split_result,param)

    if spark_inner==1:#如果当前接口启动的spark，那么要停止
        spark.stop()
        logger_info.info("spark stop")
    return status