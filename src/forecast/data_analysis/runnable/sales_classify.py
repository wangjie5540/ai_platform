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
import sys
import traceback
import logging
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config


def load_params(sdate, edate,input_table, output_table, input_partition_name, output_partition_names, col_qty, task_id):
    """运行run方法时"""
    param_cur = {
        'sdate': sdate,#'20210101',
        'edate': edate,#'20220101',
        'input_table': input_table,#'ai_dm_dev.sales_boxcox_0620',
        'output_table': output_table,#'ai_dm_dev.sales_classify_0620',
        'input_partition_name': input_partition_name, #'shop_id',
        'output_partition_names':output_partition_names, # ['shop_id'],
        'col_qty': col_qty,#'th_y',
        'task_id': task_id,#'forecast_days'

    }
    params_all = get_config(os.getcwd()+"/forecast/data_analysis/config/param.toml")
    # 获取项目1配置参数
    params = params_all['params']
    params.update(param_cur)
    return params


def run(sdate, edate, input_table, output_table, input_partition_name, output_partition_names, col_qty, task_id, spark):
    """
    跑接口
    :return:
    """
    logger_info = setup_console_log()
    setup_logging(info_log_file="sales_classify.info", error_log_file="")
    logging.info("LOADING···")

    param = load_params(sdate, edate,input_table, output_table, input_partition_name, output_partition_names, col_qty, task_id)
    logging.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logging.info("RUNNING···")
            sales_classify(spark, param)
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
    sdate, edate, input_table, output_table, input_partition_name, output_partition_names, col_qty, task_id = \
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8]
    run(sdate, edate, input_table, output_table, input_partition_name, output_partition_names, col_qty, task_id, spark=None)
