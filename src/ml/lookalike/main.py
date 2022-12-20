#!/usr/bin/env python3
# encoding: utf-8
import argparse
import json

from lookalike_model_train import start_model_train


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    parser.add_argument("--data_input", type=str, required=True, help="上游组件传输参数")
    parser.add_argument("--data_input", type=str, required=True, help="上游组件传输参数")
    parser.add_argument("--data_input", type=str, required=True, help="上游组件传输参数")
    parser.add_argument("--data_input", type=str, required=True, help="上游组件传输参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["ml-lookalike"][args.name]
    train_data_table_name = component_params["train_data_table_name"]
    test_data_table_name = component_params["test_data_table_name"]
    user_data_table_name = component_params["user_data_table_name"]
    hdfs_path = component_params["hdfs_path"]
    train_data_columns = component_params["train_data_columns"]
    user_data_columns = component_params["user_data_columns"]
    dnn_hidden_units = component_params["dnn_hidden_units"]
    dnn_dropout = component_params["dnn_dropout"]
    batch_size = component_params["batch_size"]
    lr = component_params["lr"]
    # TODO：讨论返回参数，user_embedding存储方式
    start_model_train(train_data_table_name, test_data_table_name, user_data_table_name, hdfs_path,
                      train_data_columns, user_data_columns,
                      dnn_hidden_units, dnn_dropout, batch_size, lr
                      )

    outputs = {

    }
    component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
