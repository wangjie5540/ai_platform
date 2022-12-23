#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
from train_deepfm import start_train


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    parser.add_argument("--train_data", type=str, required=True, help="训练数据")
    parser.add_argument("--test_data", type=str, required=True, help="测试数据")
    parser.add_argument("--other_data", type=str, required=True, help="其他数据")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["ml-gaoqian"][args.name]

    train_data_table_name = args.train_data['table_name']
    test_data_table_name = args.test_data['table_name']
    train_data_columns = args.train_data['column_list']
    hdfs_path = args.other_data['path']
    lr = component_params["lr"]
    weight_decay = component_params["weight_decay"]
    batch_size = component_params["batch_size"]
    start_train(train_data_table_name, train_data_columns, test_data_table_name, hdfs_path, lr, weight_decay, batch_size)

    outputs = {

    }



if __name__ == '__main__':
    run()
