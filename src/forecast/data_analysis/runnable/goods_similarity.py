# -*- coding: utf-8 -*-
# @Time : 2022/07/12
# @Author : Hunter
# -*- coding:utf-8  -*-


import argparse
import ast
import pandas as pd
import forecast.data_analysis.pd.goods_similarity_pd as similarity
from digitforce.aip.common.logging_config import setup_console_log
import logging


def run():
    setup_console_log()
    # 解析输入参数
    parser = argparse.ArgumentParser()
    parser.add_argument('-ga', '--goods_A', type=str, default="goods_A_info.csv", help='input goods A info')
    parser.add_argument('-gb', '--goods_B', type=str, default="goods_B_info.csv", help='input goods B info')
    parser.add_argument('-k', '--keys', type=ast.literal_eval, default=['shopid', 'goodsid'], help='input group keys')
    parser.add_argument('-c', '--cols', type=ast.literal_eval, default={'catg_s_id': 0.9, 'catg_m_id': 0.7,
                                                                        'catg_l_id': 0.5}, help='input '
                                                                                                'category'
                                                                                                'similarity')
    parser.add_argument('-o', '--output_file', type=str, default="similarity.csv", help='output filename')
    args = parser.parse_args()
    goods_A = pd.read_csv(args.goods_A)
    goods_B = pd.read_csv(args.goods_B)
    goods_similarity = similarity.comp_category_similarity(goods_A, goods_B, keys=args.keys, catg_cols=args.cols, s='similarity')
    goods_similarity.to_csv(args.output_file, index=False)


if __name__ == '__main__':
    run()
