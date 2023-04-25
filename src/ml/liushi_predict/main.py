#!/usr/bin/env python3
# encoding: utf-8
import argparse
import os
import digitforce.aip.common.utils.component_helper as component_helper
# 初始化组件
component_helper.init_config()
from digitforce.aip.common.utils.argument_helper import df_argument_helper
from model_predict import start_model_predict


def run():
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=True, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=True, help="名称")
    df_argument_helper.add_argument("--predict_table_name", type=str, required=False, help="预测数据")
    df_argument_helper.add_argument("--model_hdfs_path", type=str, required=False, help="模型地址")
    df_argument_helper.add_argument("--output_file_name", type=str, required=False, help="输出文件名称")
    df_argument_helper.add_argument("--instance_id", type=str, required=False, help="实例ID")
    df_argument_helper.add_argument("--predict_score_table_name", type=str, required=False, help="预测结果存储表")
    df_argument_helper.add_argument("--shapley_table_name", type=str, required=False, help="shap结果存储表")

    predict_table_name = df_argument_helper.get_argument("predict_table_name")
    model_hdfs_path = df_argument_helper.get_argument("model_hdfs_path")
    output_file_name = df_argument_helper.get_argument("output_file_name")
    instance_id = df_argument_helper.get_argument("instance_id")
    predict_score_table_name = df_argument_helper.get_argument("predict_score_table_name")
    shapley_table_name = df_argument_helper.get_argument("shapley_table_name")

    print(f"predict_table_name:{predict_table_name}")
    print(f"model_hdfs_path:{model_hdfs_path}")
    print(f"output_file_name:{output_file_name}")
    print(f"instance_id:{instance_id}")
    print(f"predict_score_table_name:{predict_score_table_name}")
    print(f"shapley_table_name:{shapley_table_name}")

    start_model_predict(predict_table_name, model_hdfs_path, output_file_name,
                        instance_id, predict_score_table_name, shapley_table_name)


if __name__ == '__main__':
    run()
