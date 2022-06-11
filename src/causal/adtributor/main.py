# encoding: utf-8

import argparse
import adtributor
from digitforce.aip.common.logging_config import setup_console_log
import logging

def run():
    setup_console_log()
    # 解析输入参数
    parser = argparse.ArgumentParser()

    parser.add_argument('--control_data_file', type=str, help='对照组数据文件')
    parser.add_argument('--experimental_data_file', type=str, help='实验组数据文件')
    parser.add_argument('--feature_list', type=list, help='待分析特征维度列表')
    parser.add_argument('--target_feature', type=list, help='目标特征，率值目标传递[分子,分母]')
    parser.add_argument('--target_is_rate', type=bool, default=True, help='目标特征是否为率值')
    parser.add_argument('--output1_file', type=str, default="Candidate.csv", help='维度集合输出文件')
    parser.add_argument('--output2_file', type=str, default="ExplanatorySet.csv", help='细分元素输出文件')

    args = parser.parse_args()
    logging.info(f"参数解析完毕. args={args}")
    adtributor(args.control_data_file, args.experimental_data_file, args.feature_list, args.target_feature, args.target_is_rate, args.output1_file, args.output2_file)

    # 向下游传递参数
    # TODO 后续将进行封装
    # component_helper.pass_output({'out_1': out_1})

if __name__ == '__main__':
    run()
