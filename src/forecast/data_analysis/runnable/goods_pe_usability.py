# -*- coding: utf-8 -*-
# @Time : 2022/07/12
# @Author : Hunter
# -*- coding:utf-8  -*-

import argparse
import ast
import pandas as pd
import forecast.data_analysis.pd.goods_pe_usability_pd as usability
from digitforce.aip.common.logging_config import setup_console_log
import logging


def run():
    setup_console_log()
    # 解析输入参数
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--goods', type=str, default="goods_info.csv", help='input goods info')
    parser.add_argument('-pef', '--price_elasticity', type=str, default="pe.csv",
                        help='input filename of goods price elasticity')
    parser.add_argument('-rc', '--r2_col', type=str, default="r2", help='input goods of fit col ')
    parser.add_argument('-pc', '--pe_col', type=str, default="pe", help='input goods price elasticity col ')
    parser.add_argument('-k', '--keys', type=ast.literal_eval, default=['shopid', 'goodsid'], help='input group keys')
    parser.add_argument('-pt', '--pe_threshold', type=float, default=-1, help='input goods pe threshold ')
    parser.add_argument('-rt', '--r2_threshold', type=float, default=0.5, help='input goods pe regression r2 threshold ')

    parser.add_argument('-ope', '--output_pe', type=str, default="pe_usable.csv", help='output pe filename')
    parser.add_argument('-ogu', '--output_goods_usable', type=str, default="pe_usable.csv", help='output pe usable '
                                                                                                 'goods filename')
    parser.add_argument('-ogun', '--output_goods_unusable', type=str, default="pe_unusable.csv", help='output pe '
                                                                                                      'unusable goods'
                                                                                                      ' filename')
    args = parser.parse_args()
    goods = pd.read_csv(args.goods)
    pe = pd.read_csv(args.price_elasticity)
    pe_usable = usability.comp_category_similarity(pe, pc=args.pe_col, rc=args.r2col, pt=args.pe_threshold, rt=args.r2_threshold)
    pe_usable.to_csv(args.output_pe, index=False)
    goods_usable = goods.merge(pe_usable, on=args.keys, how='inner')
    goods_usable[goods.columns].to_csv(args.output_goods_usable, index=False)
    goods_unusable = pd.concat([goods, goods_usable]).drop_duplicates(keep=False)
    goods_unusable.to_csv(args.output_goods_unusable, index=False)


if __name__ == '__main__':
    run()
