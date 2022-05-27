#!/usr/bin/env python3
# encoding: utf-8

import argparse
import lookalike
from facade.components.ml.lookalike import *
import os


def run():
    print("lookalike component running")
    from digitforce.aip.common.logging_config import setup_console_log
    setup_console_log()
    # 解析输入参数
    parser = argparse.ArgumentParser()
    parser.add_argument('--user_embedding_path', type=str, required=True, help='用户向量')
    parser.add_argument('--seed_file_path', type=str, required=True, help='种子用户')
    parser.add_argument('--crowd_file_path', type=str, required=True, help='待拓展人群')
    parser.add_argument('--output_file_path', type=str, required=True, help='人群结果存放路径')
    args = parser.parse_args()
    print(f"参数解析完毕. [user_embedding_path={args.user_embedding_path},"
          f"seed_file_path={args.seed_file_path},"
          f"crowd_file_path={args.crowd_file_path},"
          f"output_file_path={args.output_file_path}]")
    lookalike.get_crowd_by_seed(args.user_embedding_path, args.seed_file_path, args.crowd_file_path, args.output_file_path)
    print(f"计算完毕. 结果已输出至{args.output_file_path}。")
    component_helper.pass_output(args.output_file_path, 1)

if __name__ == '__main__':
    run()