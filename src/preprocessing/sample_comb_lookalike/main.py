#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
from src.preprocessing.sample_comb_lookalike.sample_comb import sample_comb


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["source.sample_comb_lookalike"]
    sample_table_name = component_params["sample_table_name"]
    user_feature_table_name = component_params["user_feature_table_name"]
    item_feature_table_name = component_params["item_feature_table_name"]
    table_name, columns, encoder_hdfs_path = sample_comb(sample_table_name, user_feature_table_name, item_feature_table_name)
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns,
        "encoder_hdfs_path": encoder_hdfs_path
    }
    component_helper.write_output(outputs)

if __name__ == '__main__':
    run()