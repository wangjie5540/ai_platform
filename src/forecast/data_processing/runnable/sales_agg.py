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
from forecast.data_processing.sp.sp_sales_agg import sales_aggregation
try:
    import findspark #使用spark-submit 的cluster时要注释掉
    findspark.init()
except:
    pass
import argparse
import traceback
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config
import logging

def load_params():
    """运行run方法时"""
    param_cur = {
        'mode_type': 'sp',
        'sdate': '20210101',
        'edate': '20220101',
        'input_table':'ai_dm_dev.no_sales_adjust_0620',
        'output_table':'ai_dm_dev.qty_aggregation_weekly_0620',
        'agg_func':"{'sales_aggregation_by_week': 'dt'}",
        'col_qty':'th_y',
        'agg_type':'solar_week'
    }
    params_all = get_config(os.getcwd()+"/forecast/data_processing/config/param.toml")
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
    parser = argparse.ArgumentParser(description='sales agg')
    parser.add_argument('--param', default=params, help='arguments')
    parser.add_argument('--spark', default=None, help='spark')
    args = parser.parse_args(args=[])
    return args


def run():
    """
    跑接口
    :return:
    """
    logger_info = setup_console_log(leve=logging.INFO)
    setup_logging(info_log_file="", error_log_file="", info_log_file_level="INFO")
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
            sales_aggregation(param)
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
