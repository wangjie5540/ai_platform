# encoding: utf-8
import argparse

import digitforce.aip.common.utils.component_helper as component_helper

component_helper.init_config()

from model_predict import start_model_predict

import json


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    parser.add_argument("--predict_feature_table_name", type=str, required=True,
                        help="预测样本数据")
    args = parser.parse_args()

    global_params = args.global_params
    predict_feature_table_name = args.predict_feature_table_name
    global_params = json.loads(global_params)

    # global_params下model_predict组件的两个变量
    model_hdfs_path = global_params[args.name]['model_hdfs_path']
    output_file_name = global_params[args.name]['output_file_name']
    # instance_id和存储表名由上游给出
    instance_id = global_params['instance_id']
    predict_table_name = global_params['predict_table_name']
    shapley_table_name = global_params['shapley_table_name']

    print(f"predict_table_name:{predict_feature_table_name}")
    print(f"model_hdfs_path:{model_hdfs_path}")
    print(f"output_file_name:{output_file_name}")
    start_model_predict(predict_feature_table_name=predict_feature_table_name
                        , model_hdfs_path=model_hdfs_path
                        , output_file_name=output_file_name
                        , instance_id=instance_id
                        , predict_table_name=predict_table_name
                        , shapley_table_name=shapley_table_name)

    outputs = {}
    # component_helper.write_output(outputs)


if __name__ == "__main__":
    run()
