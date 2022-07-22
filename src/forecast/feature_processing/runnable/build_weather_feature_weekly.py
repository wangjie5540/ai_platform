# -*- coding: utf-8 -*-
# @Time : 2022/05/28
# @Author : Arvin
from digitforce.aip.common.spark_helper import *
from forecast.feature_processing.sp.weather_features import build_weather_weekly_feature
import os
import sys
import traceback
import logging
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config


def load_params(sdate, edate, weather_list, col_key, join_key):
    """运行run方法时"""
    param_cur = {
        'sdate': sdate,# '20210101',
        'edate': edate,#'20220101',
        'weather_list': weather_list,#['aqi', 'hightemperature', 'hightemperature'],
        'col_key': col_key,#['province', 'city', 'district', 'week_dt', 'week'],
        'join_key': join_key,#['province', 'city', 'district']
    }

    params_all = get_config(os.getcwd()+"/forecast/feature_processing/config/param.toml")
    # 获取项目1配置参数
    params = params_all['feature_param']
    params.update(param_cur)
    return params


def run(sdate, edate, weather_list, col_key, join_key):
    """
    跑接口
    :return:
    """
    logger_info = setup_console_log()
    setup_logging(info_log_file="build_weather_feature_weekly.info", error_log_file="", info_log_file_level="INFO")
    logging.info("LOADING···")
    param = load_params(sdate, edate, weather_list, col_key, join_key)
    logging.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logging.info("RUNNING···")
            build_weather_weekly_feature(param)
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
    sdate, edate, weather_list, col_key, join_key = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    run(sdate, edate, weather_list, col_key, join_key)
