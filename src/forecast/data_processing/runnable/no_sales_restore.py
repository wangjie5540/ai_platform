# -*- coding: utf-8 -*-
# @Time : 2022/05/26
# @Author : Arvin
"""
无销量还原天级别销量还原
"""
import os
from forecast.data_processing.sp.sp_data_adjust import no_sales_adjust
import logging
import sys
import traceback
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config
import zipfile
from digitforce.aip.common.spark_init import forecast_spark_session

def load_params(sdate,edate,col_openinv,col_endinv,col_category,col_time,w,col_qty,join_key):
    """运行run方法时"""
    param_cur = {
        'sdate': sdate,#'20210101',
        'edate': edate,#'20220101',
        'col_openinv': col_openinv,#'opening_inv',
        'col_endinv': col_endinv,#'ending_inv',
        'col_category': col_category,#'category4_code',
        'col_time': col_time,#'dt',
        'w': w, #2,
        'col_qty': col_qty,#'fill_sum_qty',
        'join_key': join_key# ['shop_id', 'goods_id', 'dt']
    }
    params_all = get_config(os.getcwd()+"/forecast/data_processing/config/param.toml")
    # 获取项目1配置参数
    params = params_all['filter_p1']
    params.update(param_cur)
    return params


def run(sdate, edate, col_openinv, col_endinv, col_category, col_time, w, col_qty, join_key, spark):
    """
    跑接口
    :return:
    """
    logger_info = setup_console_log()
    setup_logging(info_log_file="no_sales_restore.info", error_log_file="", info_log_file_level="INFO")
    logging.info("LOADING···")

    param = load_params(sdate,edate,col_openinv,col_endinv,col_category,col_time,w,col_qty,join_key)
    logging.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logging.info("RUNNING···")
            no_sales_adjust(spark, param)
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
    sdate, edate, col_openinv, col_endinv, col_category, col_time, w, col_qty, join_key = sys.argv[1], sys.argv[2],sys.argv[3], sys.argv[4],\
                                                                      sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9]
    run(sdate, edate, col_openinv, col_endinv, col_category, col_time, w, col_qty, join_key, spark)