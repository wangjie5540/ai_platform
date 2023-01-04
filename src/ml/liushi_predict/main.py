#!/usr/bin/env python3
# encoding: utf-8
import argparse
from model_predict import start_model_predict


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    parser.add_argument("--predict_table_name", type=str, required=True, help="预测数据")
    parser.add_argument("--model_hdfs_path", type=str, required=True, help="模型地址")

    args = parser.parse_args()

    start_model_predict(args.predict_table_name, args.model_hdfs_path)

    outputs = {

    }
    # component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
