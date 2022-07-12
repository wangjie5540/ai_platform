# -*- coding: utf-8 -*-
# @Time : 2022/07/12
# @Author : Hunter
# -*- coding:utf-8  -*-


import argparse
import ast
import pandas as pd
import forecast.data_analysis.pd.time_series_features_pd as tsf
from digitforce.aip.common.logging_config import setup_console_log
import logging


def run():
    setup_console_log()
    # 解析输入参数
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--sales', type=str, default="sales.csv", help='input goods sales')
    parser.add_argument('-gb', '--goods_B', type=str, default="goods_B_info.csv", help='input goods B info')
    parser.add_argument('-k', '--keys', type=ast.literal_eval, default=['shopid', 'goodsid'], help='input group keys')
    parser.add_argument('-f', '--features', type=ast.literal_eval, default=None, help='input time series features')
    parser.add_argument('-t', '--ts_col', type=ast.literal_eval, default=None, help='input time series columns')
    parser.add_argument('-y', '--sales_col', type=str, default="qty", help='input goods sales colulmn')

    parser.add_argument('-o', '--output_file', type=str, default="ts_features.csv", help='output filename')
    args = parser.parse_args()
    df = pd.read_csv(args.sales)
    ts_features = tsf.comp_time_series_features(df, f_cols=args.features, keys=args.keys, ts_col=args.ts_col, y=args.sales_col)
    ts_features.to_csv(args.output_file, index=False)


if __name__ == '__main__':
    run()
