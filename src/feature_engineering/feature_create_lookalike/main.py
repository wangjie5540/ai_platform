#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json

from src.feature_engineering.feature_create_lookalike.feature_create import feature_create


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["feature_engineering.feature_create_lookalike"]
    data_table_name = component_params["data_table_name"]
    columns = component_params["columns"]
    # TODO：重复使用的参数如何放置？
    event_code = component_params["event_code"]
    sample_table_name = component_params["sample_table_name"]
    user_feature_table_name, item_feature_table_name = feature_create(data_table_name, columns, event_code, sample_table_name)
    outputs = {
        "type": "hive_table",
        "user_feature_table_name": user_feature_table_name,
        "item_feature_table_name": item_feature_table_name
    }
    component_helper.write_output(outputs)

if __name__ == '__main__':
    run()