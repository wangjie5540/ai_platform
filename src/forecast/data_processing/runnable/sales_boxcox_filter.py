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
from forecast.data_processing.sp.sp_sales_filter import sales_filter_by_boxcox
import logging
try:
    import findspark  # 使用spark-submit 的cluster时要注释掉

    findspark.init()
except:
    pass
import sys
import traceback
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config


def load_params(sdate, edate, col_qty):
    """运行run方法时"""
    param_cur = {
        'sdate': sdate, #'20210101',
        'edate': edate, #'20220101',
        'col_qty': col_qty #'th_y'

    }
    params_all = get_config(os.getcwd() + "/forecast/data_processing/config/param.toml")
    # 获取项目1配置参数
    params = params_all['filter_p1']
    params.update(param_cur)
    return params


def run(sdate, edate, col_qty):
    """
    跑接口
    :return:
    """
    logger_info = setup_console_log(leve=logging.INFO)
    setup_logging(info_log_file="", error_log_file="", info_log_file_level="INFO")
    logging.info("LOADING···")
    param = load_params(sdate, edate, col_qty)
    logging.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logging.info("RUNNING···")
            sales_filter_by_boxcox(param)
        else:
            # pandas版本
            pass
        status = "SUCCESS"
        logger_info.info("SUCCESS")
    except Exception as e:
        status = "ERROR"
        logging.info(traceback.format_exc())
    return status


if __name__ == "__main__":
    sdate, edate, col_qty = sys.argv[1], sys.argv[2], sys.argv[3]
    run(sdate, edate, col_qty)
