# -*- coding: utf-8 -*-
# @Time : 2022/07/05
# @Author : Arvin
# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
大单过滤
"""
import os
from forecast.data_analysis.sp.sales_classify_sp import sales_classify

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
        'mode_type': 'sp',
        'sdate': '20210101',
        'edate': '20220101',
        'input_table': 'ai_dm_dev.sales_boxcox_0620',
        'output_table': 'ai_dm_dev.sales_classify_0620',
        'input_partition_name': 'shop_id',
        'output_partition_names': ['shop_id'],
        'col_qty': 'th_y'

    }
    f = TomlOperation(os.getcwd()+"/forecast/data_analysis/config/param.toml")
    params_all = f.read_file()
    # 获取项目1配置参数
    params = params_all['params']
    params.update(param_cur)
    return params


def parse_arguments():
    """
    #开发测试用
    :return:
    """
    params = load_params()
    parser = argparse.ArgumentParser(description='sales classify')
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
            sales_classify(spark, param)
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
