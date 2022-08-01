# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    时序模型：对外提供的接口
"""
import ast
import logging
import os

try:
    import findspark  # 使用spark-submit 的cluster时要注释掉

    findspark.init()
except:
    pass
import sys
import json
import argparse
import traceback

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(file_path)  # 解决不同位置调用依赖包路径问题
from forecast.time_series.sp.predict_for_time_series_sp import predict_sp
from digitforce.aip.common.file_config import get_default_conf
from digitforce.aip.common.data_helper import update_param_default
from digitforce.aip.common.logging_config import setup_console_log, setup_logging

import findspark

findspark.init()


def time_series_predict(param, spark=None):
    """
    #时序模型预测
    :param param: 所需参数
    :param spark: spark，如果不传入则会内部启动一个运行完关闭
    :return:成功：True 失败：False
    """
    setup_console_log(level=logging.INFO)
    setup_logging(info_log_file="time_series_predict.info", error_log_file="", info_log_file_level="INFO")
    logging.info("=============================LOADING==================================")
    mode_type = 'sp'  # 先给个默认值
    if 'mode_type' in param.keys():
        mode_type = param['mode_type']
    try:
        if mode_type == 'sp':  # spark版本
            logging.info("RUNNING.......")
            status = predict_sp(param, spark)
        else:  # pandas版本
            pass
        logging.info("SUCCESS!")
    except Exception as e:
        logging.info(traceback.format_exc())
        status = "FAIL"
    return status


def run(forecast_start_date, purpose, time_type, feat_y, output_table, col_qty, cols_feat_y,toml_list,seaction, spark):
    """
    跑接口
    :return:
    """
    # pipline input
    # 'forecast_start_date': '20211207',
    # 'purpose': 'predict',
    # 'time_type': 'day',
    # 'feat_y':y值表
    # 'output_table':结果表
    param = {"forecast_start_date": forecast_start_date,
             "purpose": purpose,  # 该参数判断是预测还是回测，存在的意义是更新配置文件中的purpose,生成更新后的param，为后续使用
             "time_type": time_type, "feat_y": feat_y,
             "output_table": output_table, "col_qty": col_qty, "cols_feat_y": cols_feat_y}

    default_conf = get_default_conf(toml_list,seaction)
    param = update_param_default(param, default_conf)
    time_series_predict(param, spark)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--forecast_start_date', type=str, default=None, help='input forecast_start_date')
    parser.add_argument('-p', '--purpose', type=str, default='predict', help='input purpose')
    parser.add_argument('-t', '--time_type', type=str, default='day', help='input time_type')
    parser.add_argument('-f_y', '--feat_y', type=str, default=None, help='input feat_y')
    parser.add_argument('-o', '--output_table', type=str, default=None, help='input output_table')
    parser.add_argument('-c', '--col_qty', type=str, default=None, help='input col_qty')
    parser.add_argument('-c_f_y', '--cols_feat_y', type=ast.literal_eval(), default=None, help='input cols_feat_y')
    parser.add_argument('-t_l', '--toml_list', type=ast.literal_eval(), default=None, help='input toml list')
    parser.add_argument('-s_l', '--seaction', type=ast.literal_eval(), default=None, help='input seaction')
    parser.add_argument('-s', '--spark', type=str, default=None, help='input spark')
    args = parser.parse_args()
    run(args.forecast_start_date, args.purpose, args.time_type, args.feat_y, args.output_table, args.col_qty,
        args.cols_feat_y, args.toml_list, args.seaction, args.spark)


if __name__ == "__main__":
    main()
