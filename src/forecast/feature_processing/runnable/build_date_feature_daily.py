# -*- coding: utf-8 -*-
# @Time : 2022/05/27
# @Author : Arvin
# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:日期特征-天
"""

from feature_process.sp.date_features import bulid_date_daily_feature

try:
    import findspark #使用spark-submit 的cluster时要注释掉
    findspark.init()
except:
    pass
import argparse
import traceback
from components.common.log import get_logger
from components.common.toml_helper import TomlOperation


def load_params():
    """运行run方法时"""
    param_cur = {
        'mode_type': 'sp',
        'sdate': '20220101',
        'edate': '20220501',
        'col_list': ['dt', 'week', 'year', 'week_day', 'is_holiday'],
        'ctype': 'pd',
        'feat_func': "{'holiday_short': (['last', 'future'], [5]), 'holiday_long': (['last', 'future'], [14]), 'is_workday': (['last'], [14])}",
        'output_table': ''
    }
    f = TomlOperation("param.toml")
    params_all = f.read_file()
    # 获取项目1配置参数
    params = params_all['feature_param']
    params.update(param_cur)
    return params


def parse_arguments():
    """
    #开发测试用
    :return:
    """
    params = load_params()
    parser = argparse.ArgumentParser(description='big order filter')
    parser.add_argument('--param', default=params, help='arguments')
    parser.add_argument('--spark', default=None, help='spark')
    args = parser.parse_args()
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

    logger_info.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logger_info.info("RUNNING···")
            bulid_date_daily_feature(spark, param)
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

