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
from digitforce.aip.common.spark_helper import *
from forecast.data_split.sp.model_selection_grouping import model_selection_grouping
import sys
import logging
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config
try:
    import findspark #使用spark-submit 的cluster时要注释掉
    findspark.init()
except:
    pass
import traceback

def load_params(sdate,edate):
    """运行run方法时"""
    param_cur = {
        'sdate': sdate,#'20210101',
        'edate': edate,#'20220101'
    }
    params_all = get_config(os.getcwd()+"/forecast/data_split/config/param.toml")
    # 获取项目1配置参数
    params_data_prepare = params_all['params_data_prepare']
    params_model_selection = params_all['model_selection']
    params_model_grouping = params_all['model_grouping']
    params_data_prepare.update(param_cur)
    params_model_selection.update(param_cur)
    params_model_grouping.update(param_cur)
    return params_data_prepare, params_model_selection, params_model_grouping


def run(sdate, edate):
    """
    跑接口
    :return:
    """
    logger_info = setup_console_log()
    setup_logging(info_log_file="data_split.info", error_log_file="", info_log_file_level="INFO")
    logging.info("LOADING···")

    params_data_prepare, params_model_selection, params_model_grouping = load_params(sdate, edate)
    logging.info(str(params_data_prepare)+str(params_model_selection)+str(params_model_grouping))
    if 'mode_type' in params_data_prepare.keys():
        run_type = params_data_prepare['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logging.info("RUNNING···")
            model_selection_grouping(params_data_prepare, params_model_selection, params_model_grouping)
        else:
            # pandas版本
            pass
        status = "SUCCESS"
        logging.info("SUCCESS")
    except Exception as e:
        status = "ERROR"
        logging.info(traceback.format_exc())
    return status


if __name__ == "__main__":
    sdate, edate = sys.argv[1], sys.argv[2]
    run(sdate, edate)
