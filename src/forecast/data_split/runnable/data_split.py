# -*- coding: utf-8 -*-
# @Time : 2022/05/23
# @Author : Arvin
# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
大单过滤
"""
import os
import sys

from forecast.data_split.sp.model_selection_grouping import model_selection_grouping

try:
    import findspark #使用spark-submit 的cluster时要注释掉
    findspark.init()
except:
    pass
import argparse
import traceback
from  forecast.common.log import get_logger
from  forecast.common.toml_helper import TomlOperation


def load_params():
    """运行run方法时"""
    param_cur = {
        'mode_type': 'sp',
        'sdate': '20210101',
        'edate': '20220101'
    }
    f = TomlOperation(os.getcwd()+"/forecast/data_split/config/param.toml")
    params_all = f.read_file()
    # 获取项目1配置参数
    params_data_prepare = params_all['params_data_prepare']
    params_model_selection = params_all['model_selection']
    params_model_grouping = params_all['model_grouping']
    params_data_prepare.update(param_cur)
    params_model_selection.update(param_cur)
    params_model_grouping.update(param_cur)
    return params_data_prepare,params_model_selection,params_model_grouping


def parse_arguments():
    """
    #开发测试用
    :return:
    """
    params_data_prepare,params_model_selection,params_model_grouping = load_params()
    parser = argparse.ArgumentParser(description='model select group')
    parser.add_argument('--params_data_prepare', default=params_data_prepare, help='arguments')
    parser.add_argument('--params_model_selection', default=params_model_selection, help='arguments')
    parser.add_argument('--params_model_grouping', default=params_model_grouping, help='arguments')
    parser.add_argument('--spark', default=spark, help='spark')
    args = parser.parse_args(args=[])
    return args


def run():
    """
    跑接口
    :return:
    """
    logger_info = get_logger()
    logger_info.info("LOADING···")
    args = parse_arguments()
    params_data_prepare = args.params_data_prepare
    params_model_selection = args.params_model_selection
    params_model_grouping = args.params_model_grouping
    spark = args.spark
    print("args", args)
    logger_info.info(str(params_data_prepare)+str(params_model_selection)+str(params_model_grouping))
    if 'mode_type' in params_data_prepare.keys():
        run_type = params_data_prepare['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logger_info.info("RUNNING···")
            model_selection_grouping(spark,params_data_prepare,params_model_selection,params_model_grouping)
        else:
            # pandas版本
            pass
        status = "SUCCESS"
        logger_info.info("SUCCESS")
    except Exception as e:
        status = "ERROR"
        logger_info.info(traceback.format_exc())
    return status


if __name__ == "__main__":
    run()
