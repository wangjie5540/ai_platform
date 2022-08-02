# -*- coding: utf-8 -*-
# @Time : 2022/07/12
# @Author : Hunter
# -*- coding:utf-8  -*-

import argparse
import ast
import pandas as pd
import forecast.data_analysis.pd.price_elasticity_pd as pe
from digitforce.aip.common.logging_config import setup_console_log
import logging


def run():
    setup_console_log()
    # 解析输入参数
    # 回归模型参数
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--sales', type=str, default="sales.csv", help='input goods history sales info')
    parser.add_argument('-sep', '--sep', type=str, default=',', help='the separator of csv data')
    parser.add_argument('-k', '--keys', type=ast.literal_eval, default=['shop_id', 'goods_id'], help='input group keys')
    parser.add_argument('-f', '--factor', type=ast.literal_eval, default=None, help='input x factors ')
    parser.add_argument('-y', '--y', type=str, default="sales_qty", help='input sales quantity col ')
    parser.add_argument('-p', '--p', type=str, default="price", help='input price col ')
    parser.add_argument('-m', '--method', type=str, default="linear", help='input price elasticity compute method ')
    parser.add_argument('-o', '--output_file', type=str, default="pe.csv", help='price elasticity output filename')

    # 商品相似度迁移模型参数
    parser.add_argument('-gs', '--goods_similarity', type=str, default="similarity.csv",
                        help='input goods similarity filename')
    parser.add_argument('-pef', '--price_elasticity', type=str, default="pe.csv",
                        help='input goods price elasticity of reg model filename')
    # parser.add_argument('-e', '--encoding', type=str, default="unicode_escape", help='the encoding of files ')
    parser.add_argument('-sk', '--sim_keys', type=ast.literal_eval, default=['goods_id'], help='input goods similar keys')
    parser.add_argument('-pc', '--pe_col', type=str, default="pe", help='input goods price elasticity col ')
    parser.add_argument('-sc', '--similarity_col', type=str, default="similarity", help='input goods similarity col ')
    parser.add_argument('-st', '--similarity_threshold', type=float, default=0.5, help='input goods similarity threshold ')
    parser.add_argument('-rc', '--r2_col', type=str, default="r2", help='input goods of fit col ')
    args = parser.parse_args()
    m = args.method
    if m == 'trans':
        sim = pd.read_csv(args.goods_similarity)
        pe_reg = pd.read_csv(args.price_elasticity)

        goods_pe = pe.compute_pe_by_transfer_model(sim, pe_reg, pe_sign=args.pe_col, s=args.similarity_col,
                                                   st=args.similarity_threshold, keys=args.keys, sk=args.sim_keys, B_sign='_B')
    else:
        sales = pd.read_csv(args.sales, args.sep)
        goods_pe = pe.compute_price_elasticity(sales, x_factor=args.factor, keys=args.keys, y=args.y, p=args.p,
                                               method=m)
    goods_pe.to_csv(args.output_file, index=False)


if __name__ == '__main__':
    run()
