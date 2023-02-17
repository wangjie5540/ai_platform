#!/usr/bin/env python3
# encoding: utf-8
import argparse
import json
import os
from digitforce.aip.common.utils import component_helper
component_helper.init_config()
from model_predict import start_model_predict
# from digitforce.aip.common.utils import component_helper
from digitforce.aip.common.utils.argument_helper import df_argument_helper

def run():
    # 参数解析
    # component_helper.init_config()
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    parser.add_argument("--sample", type=str, required=False, help="预测数据")
    # df_argument_helper.add_argument("--model_hdfs_path", type=str, required=False, help="模型地址")
    # df_argument_helper.add_argument("--output_file_name", type=str, required=False, help="输出文件名称")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    predict_table_name_params = json.loads(args.sample)
    predict_table_name = predict_table_name_params['table_name']
    model_hdfs_path = global_params[args.name]["model_hdfs_path"]
    output_file_name = global_params[args.name]["output_file_name"]
    print(f"predict_table_name:{predict_table_name}")
    print(f"model_hdfs_path:{model_hdfs_path}")
    print(f"output_file_name:{output_file_name}")
    start_model_predict(predict_table_name, model_hdfs_path, output_file_name)

    outputs = {

    }
    # component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
