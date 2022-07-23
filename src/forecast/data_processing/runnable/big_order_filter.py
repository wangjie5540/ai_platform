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
import sys
import os
from forecast.data_processing.sp.sp_sales_filter import big_order_filter
import logging
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config
import traceback


def load_params(sdate, edate):
    param_cur = {
        'sdate': sdate,
        'edate': edate
    }
    params_all = get_config(os.getcwd()+"/forecast/data_processing/config/param.toml")
    # 获取项目1配置参数
    params = params_all['filter_p1']
    params.update(param_cur)
    return params


def run(sdate, edate, spark):
    """
    跑接口
    :return:
    """
    logger_info = setup_console_log()
    setup_logging(info_log_file="big_order_filter.info", error_log_file="", info_log_file_level="INFO")
    logging.info("LOADING···")
    param = load_params(sdate, edate)
    logging.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logging.info("RUNNING···")
            big_order_filter(spark, param)
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
