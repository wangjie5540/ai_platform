import argparse
from model import shapley
from digitforce.aip.common.logging_config import setup_console_log
import logging


def run():
    setup_console_log()
    # 解析输入参数
    parser = argparse.ArgumentParser()

    # 输入data.csv样例：
    # path;total_conversions
    # Email;110
    # Email > Facebook;11
    # Email > Facebook > House > Ads;8
    # Facebook > House > Ads;72
    # Facebook > House > Ads > Instagram;24

    parser.add_argument('--input_file', type=str, default="data.csv", help='input filename')
    parser.add_argument('--output_file', type=str, default="output.txt", help='output filename')

    # 模型1: simply，基于传统shapley的简化，原文：https://arxiv.org/abs/1804.05327，实现参考：https://github.com/ianchute/shapley-attribution-model-zhao-naive
    # 模型2: ordered，在simply基础上计算每个渠道在各个触点上的贡献度，从1维扩展到n维，实现参考同上
    parser.add_argument('--model_type', type=str, default="simply", help='simply or ordered')

    args = parser.parse_args()
    logging.info(f"参数解析完毕. args={args}")
    shapley(args.input_file, args.output_file, args.model_type)

    # 向下游传递参数
    # TODO 后续将进行封装
    # component_helper.pass_output({'out_1': out_1})


if __name__ == '__main__':
    run()
