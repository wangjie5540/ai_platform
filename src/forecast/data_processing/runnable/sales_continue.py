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
import logging
from forecast.data_processing.sp.sp_sales_agg import sales_continue_processing
import traceback
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config
import zipfile
from digitforce.aip.common.spark_init import forecast_spark_session



def load_params(sdate,edate,date_type,col_time,col_wm,input_table,output_table,col_qty):
    """运行run方法时"""
    param_cur = {

        'sdate': sdate,#'20210101',
        'edate': edate,#'20211231',
        'date_type': date_type,#'month', #'day'/'week'/'month'
        'col_time': col_time,#'dt',
        'col_wm': col_wm,#'solar_month',
        'input_table': input_table,#'ai_dm_dev.qty_aggregation_monthly_0620',
        'output_table': output_table,#'ai_dm_dev.qty_aggregation_monthly_continue_0620',
        'col_qty': col_qty,#'sum_th_y'
    }
    params_all= get_config(os.getcwd()+"/forecast/data_processing/config/param.toml")
    # 获取项目1配置参数
    params = params_all['filter_p1']
    params.update(param_cur)
    return params


def run(sdate, edate,date_type, col_time, col_wm, input_table, output_table, col_qty, spark):
    """
    跑接口
    :return:
    """
    logger_info = setup_console_log()
    setup_logging(info_log_file="sale_continue.info", error_log_file="", info_log_file_level="INFO")
    logging.info("LOADING···")
    param = load_params(sdate,edate,date_type,col_time,col_wm,input_table,output_table,col_qty)
    logging.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logging.info("RUNNING···")
            sales_continue_processing(spark, param)
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
    files1 = zipfile.ZipFile('./forecast.zip', 'r')
    files2 = zipfile.ZipFile('./digitforce.zip', 'r')
    files1.extractall(os.getcwd())
    files2.extractall(os.getcwd())
    spark = forecast_spark_session("submit_test")
    sdate, edate,date_type, col_time, col_wm, input_table, output_table, col_qty = sys.argv[1], sys.argv[2], sys.argv[3], \
                                                                                   sys.argv[4], sys.argv[5], sys.argv[6], \
                                                                                   sys.argv[7], sys.argv[8]
    run(sdate, edate,date_type, col_time, col_wm, input_table, output_table, col_qty, spark=None)
