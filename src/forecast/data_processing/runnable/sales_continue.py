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
from forecast.data_processing.sp.sp_sales_agg import sales_continue_processing

try:
    import findspark #使用spark-submit 的cluster时要注释掉
    findspark.init()
except:
    pass
import argparse
import traceback
from forecast.common.log import get_logger
from forecast.common.toml_helper import TomlOperation




def load_params():
    """运行run方法时"""
    param_cur = {
        'data_type': 'sp',
        'sdate': '20210101',
        'edate': '20211231',
        'date_type': 'month', #'day'/'week'/'month'
        'col_key': ['shop_id', 'goods_id'],
        'col_time': 'dt',
        'col_wm':'solar_month',
        'input_table': 'ai_dm_dev.qty_aggregation_monthly_0620',
        'output_table': 'ai_dm_dev.qty_aggregation_monthly_continue_0620',
        'col_qty': 'sum_th_y'
    }
    f = TomlOperation(os.getcwd()+"/forecast/data_processing/config/param.toml")
    params_all = f.read_file()
    # 获取项目1配置参数
    params = params_all['filter_p1']
    params.update(param_cur)
    return params


def parse_arguments():
    """
    #开发测试用
    :return:
    """
    params = load_params()
    parser = argparse.ArgumentParser(description='sales fill zero')
    parser.add_argument('--param', default=params, help='arguments')
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
    param = args.param
    spark = args.spark
    print("args", args)
    logger_info.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logger_info.info("RUNNING···")
            sales_continue_processing(spark, param)
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
