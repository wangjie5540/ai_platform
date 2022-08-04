# -*- coding: utf-8 -*-
# @Time : 2022/05/27
# @Author : Arvin
# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
周销量特征
"""
from forecast.feature_processing.sp.sale_features import build_sales_features_weekly
import os
import sys
import traceback
import logging
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config
from digitforce.aip.common.spark_init import forecast_spark_session
import zipfile

def load_params(sdate, edate, col_time, col_qty, input_table, output_table):
    """运行run方法时"""
    param_cur = {
        'sdate': sdate,#'20210101',
        'edate': edate,#'20210501',
        'col_time': col_time,#'dt',
        'col_qty': col_qty,#'sum_th_y',
        'input_table': input_table,#'ai_dm_dev.qty_aggregation_weekly_continue_0620',
        'output_table': output_table,#'ai_dm_dev.sales_features_weekly_0620'
    }

    params_all = get_config(os.getcwd()+"/forecast/feature_processing/config/param.toml")
    # 获取项目1配置参数
    params = params_all['feature_param']
    params.update(param_cur)
    return params


def run(sdate, edate, col_time, col_qty, input_table, output_table, spark):
    """
    跑接口
    :return:
    """
    logger_info = setup_console_log()
    setup_logging(info_log_file="build_sales_feature_weekly.info", error_log_file="", info_log_file_level="INFO")
    logging.info("LOADING···")
    param = load_params(sdate, edate, col_time, col_qty, input_table, output_table)
    logging.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logging.info("RUNNING···")
            build_sales_features_weekly(spark, param)
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
    spark = forecast_spark_session("weather_featuer")
    sdate, edate, col_time, col_qty, input_table, output_table = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],\
                                                                 sys.argv[5], sys.argv[6]
    run(sdate, edate, col_time, col_qty,spark)
