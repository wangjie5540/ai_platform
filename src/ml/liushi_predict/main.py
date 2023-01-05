#!/usr/bin/env python3
# encoding: utf-8
import argparse
import os

from model_predict import start_model_predict

from digitforce.aip.common.utils.argument_helper import df_argument_helper

def run():
    # 参数解析

    df_argument_helper.add_argument("--global_params", type=str, required=True, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=True, help="名称")
    df_argument_helper.add_argument("--predict_table_name", type=str, required=False, help="预测数据")
    df_argument_helper.add_argument("--model_hdfs_path", type=str, required=False, help="模型地址")

    predict_table_name = df_argument_helper.get_argument("predict_table_name")
    model_hdfs_path = df_argument_helper.get_argument("model_hdfs_path")
    print(f"predict_table_name:{predict_table_name}")
    print(f"model_hdfs_path:{model_hdfs_path}")
    start_model_predict(predict_table_name, model_hdfs_path)

    outputs = {

    }
    # component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
