#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
from sample_comb import sample_comb


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["preprocessing.sample_comb_lookalike"]
    sample_table_name = component_params["sample_table_name"]
    sample_columns = component_params["sample_columns"]
    user_feature_table_name = component_params["user_feature_table_name"]
    user_columns = component_params["user_columns"]
    item_feature_table_name = component_params["item_feature_table_name"]
    item_columns = component_params["item_columns"]
    train_data_table_name, test_data_table_name, user_data_table_name, hdfs_dir = sample_comb(sample_table_name,
                                                                                              sample_columns,
                                                                                              user_feature_table_name,
                                                                                              user_columns,
                                                                                              item_feature_table_name,
                                                                                              item_columns)

    outputs = {
        "type": "hive_table",
        "train_data_table_name": train_data_table_name,
        "test_data_table_name": test_data_table_name,
        "user_data_table_name": user_data_table_name,
        "hdfs_dir": hdfs_dir
    }
    component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
