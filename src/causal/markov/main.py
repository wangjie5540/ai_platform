
import argparse
from model import markov
from digitforce.aip.common.logging_config import setup_console_log
import logging

def run():
    setup_console_log()
    # 解析输入参数
    parser = argparse.ArgumentParser()

    # data.csv样例：
    # path;total_conversions
    # Email;110
    # Email > Facebook;11
    # Email > Facebook > House > Ads;8
    # Facebook > House > Ads;72
    # Facebook > House > Ads > Instagram;24

    parser.add_argument('--input_file', type=str, default="data.csv", help='input filename')
    parser.add_argument('--output_file', type=str, default="output.txt", help='output filename')

    args = parser.parse_args()
    logging.info(f"参数解析完毕. args={args}")
    markov(args.input_file, args.output_file)

    # 向下游传递参数
    # TODO 后续将进行封装
    # component_helper.pass_output({'out_1': out_1})

if __name__ == '__main__':
    run()
