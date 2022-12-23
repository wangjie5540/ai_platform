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
    train_data = json.loads(args.train_data)
    test_data = json.loads(args.test_data)
    user_data = json.loads(args.user_data)
    other_data = json.loads(args.other_data)
    component_params = global_params[args.name]
    dnn_dropout = component_params["dnn_dropout"]
    batch_size = component_params["batch_size"]
    lr = component_params["lr"]

    # TODO：讨论返回参数，user_embedding存储方式
    start_model_train(train_data['table_name'], test_data['table_name'], user_data['table_name'],
                      other_data['path'], train_data['column_list'], user_data['column_list'],
                      dnn_dropout=dnn_dropout, batch_size=batch_size, lr=lr)

    outputs = {

    }
    # component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
