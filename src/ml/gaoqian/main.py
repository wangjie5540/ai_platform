#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
from src.ml.gaoqian.train_deepfm import start_train


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["preprocessing.sample_comb_lookalike"]
    train_data_table_name = component_params["train_data_table_name"]
    test_data_table_name = component_params["test_data_table_name"]
    train_data_columns = component_params["train_data_columns"]
    hdfs_path = component_params["hdfs_path"]
    lr = component_params["lr"]
    weight_decay = component_params["weight_decay"]
    start_train(train_data_table_name, train_data_columns, test_data_table_name, hdfs_path, lr, weight_decay)

    outputs = {
        "type": "hive_table",
        "train_data_table_name": train_data_table_name,
        "test_data_table_name": test_data_table_name,
        "hdfs_dir": hdfs_dir
    }
    component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
