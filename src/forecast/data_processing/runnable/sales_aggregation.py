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
from digitforce.aip.common.spark_helper import *
from forecast.data_processing.sp.sp_sales_agg import sales_aggregation
import logging
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_config
try:
    import findspark #使用spark-submit 的cluster时要注释掉
    findspark.init()
except:
    pass
import traceback



def load_params(sdate, edate,input_table,output_table,agg_func,col_qty,agg_type):
    """运行run方法时"""
    param_cur = {
        'sdate': sdate,
        'edate': edate,
        'input_table': input_table, #'ai_dm_dev.outlier_order_0620',
        'output_table': output_table, #'ai_dm_dev.qty_aggregation_0620',
        'agg_func': agg_func, #"{'sales_aggregation_by_day': 'dt'}",
        'col_qty': col_qty, #'qty',
        'agg_type': agg_type #'day'
    }
    params_all = get_config(os.getcwd()+"/forecast/data_processing/config/param.toml")
    # 获取项目1配置参数
    params = params_all['filter_p1']
    params.update(param_cur)
    return params


def run(sdate, edate,input_table,output_table,agg_func,col_qty,agg_type):
    """
    跑接口
    :return:
    """
    logger_info = setup_console_log()
    setup_logging(info_log_file="big_order_filter.info", error_log_file="", info_log_file_level="INFO")
    logging.info("LOADING···")
    param = load_params(sdate, edate,input_table,output_table,agg_func,col_qty,agg_type)
    logging.info(str(param))
    if 'mode_type' in param.keys():
        run_type = param['mode_type']
    else:
        run_type = 'sp'
    try:
        if run_type == 'sp':  # spark版本
            logging.info("RUNNING···")
            sales_aggregation(param)
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
    sdate, edate,input_table,output_table,agg_func,col_qty,agg_type = sys.argv[1], sys.argv[2],sys.argv[3], sys.argv[4],\
                                                                      sys.argv[5], sys.argv[6], sys.argv[7]
    run(sdate, edate,input_table,output_table,agg_func,col_qty,agg_type)
