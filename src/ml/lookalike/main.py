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
    parser.add_argument("--train_data", type=str, required=True, help="训练数据")
    parser.add_argument("--test_data", type=str, required=True, help="测试数据")
    parser.add_argument("--user_data", type=str, required=True, help="用户数据")
    parser.add_argument("--other_data", type=str, required=True, help="其他数据")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["ml-lookalike"][args.name]
    dnn_hidden_units = component_params["dnn_hidden_units"]
    dnn_dropout = component_params["dnn_dropout"]
    batch_size = component_params["batch_size"]
    lr = component_params["lr"]

    # TODO：讨论返回参数，user_embedding存储方式
    start_model_train(args.train_data['table_name'], args.test_data['table_name'], args.user_data['table_name'],
                      args.other_data['path'], args.train_data['column_list'], args.user_data['column_list'],
                      dnn_hidden_units, dnn_dropout, batch_size, lr)

    outputs = {

    }
    # component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
