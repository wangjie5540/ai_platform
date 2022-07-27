# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：对外提供的接口：回测
"""
import argparse
import ast
import logging
import os

try:
    import findspark  # 使用spark-submit 的cluster时要注释掉

    findspark.init()
except:
    pass
import sys
import traceback

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(file_path)

from forecast.time_series.sp.backup_test_for_time_series_sp import back_test_sp
from digitforce.aip.common.logging_config import setup_console_log, setup_logging
from digitforce.aip.common.file_config import get_default_conf
from digitforce.aip.common.data_helper import update_param_default


def time_series_back_test(param, spark=None):
    """
    预测模型回测
    :param param: 所需参数
    :param spark: spark，如果不传入则会内部启动一个运行完关闭
    :return:成功：True 失败：False
    """

    setup_console_log(level=logging.INFO)
    setup_logging(info_log_file="time_series_back_test.info", error_log_file="", info_log_file_level="INFO")
    logging.info("==============================LOADING============================")
    mode_type = 'sp'  # 先给个默认值
    status = False
    if 'mode_type' in param.keys():
        mode_type = param['mode_type']
    try:
        if mode_type == 'sp':  # spark版本
            logging.info("RUNNING......")
            status = back_test_sp(param, spark)
        else:  # pandas版本
            pass

        logging.info("SUCCESS")
    except Exception as e:
        logging.info(traceback.format_exc())
        status = "FAIL"
    return status


def run(forecast_start_date, purpose, time_type, feat_y, output_table, col_qty, cols_feat_y, output_back_test_table,
        spark):
    """
    跑接口
    :return:
    """
    param = {"forecast_start_date": forecast_start_date, "purpose": purpose, "time_type": time_type, "feat_y": feat_y,
             "output_table": output_table, "col_qty": col_qty, "cols_feat_y": cols_feat_y,
             "output_back_test_table": output_back_test_table}
    default_conf = get_default_conf()
    param = update_param_default(param, default_conf)
    time_series_back_test(param, spark)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--forecast_start_date', type=str, default=None, help='input forecast_start_date')
    parser.add_argument('-p', '--purpose', type=str, default='back_test', help='input purpose')
    parser.add_argument('-t', '--time_type', type=str, default='day', help='input time_type')
    parser.add_argument('-f_y', '--feat_y', type=str, default=None, help='input feat_y')
    parser.add_argument('-o', '--output_table', type=str, default=None, help='input output_table')
    parser.add_argument('-c', '--col_qty', type=str, default=None, help='input col_qty')
    parser.add_argument('-c_f_y', '--cols_feat_y', type=ast.literal_eval(), default=None, help='input cols_feat_y')
    parser.add_argument('-s', '--spark', type=str, default=None, help='input spark')
    parser.add_argument('o_b', '--output_back_test_table', type=str, default=None, help='input output_back_test_table')

    args = parser.parse_args()
    run(args.forecast_start_date, args.purpose, args.time_type, args.feat_y, args.output_table, args.col_qty,
        args.cols_feat_y, args.output_back_test_table, args.spark)


if __name__ == "__main__":
    main()
