#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json

from src.feature_engineering.feature_create_gaoqian.feature_create import feature_create


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["feature_engineering.feature_create_lookalike"]
    event_table_name = component_params["event_table_name"]
    event_columns = component_params["event_columns"]
    item_table_name = component_params["item_table_name"]
    item_columns = component_params["item_columns"]
    user_table_name = component_params["user_table_name"]
    user_columns = component_params["user_columns"]
    event_code_list = component_params["event_code"]
    sample_table_name = component_params["sample_table_name"]
    category = component_params['category']
    user_feature_table_name = feature_create(event_table_name, event_columns, item_table_name, item_columns, user_table_name, user_columns, event_code_list, category, sample_table_name)
    outputs = {
        "type": "hive_table",
        "user_feature_table_name": user_feature_table_name
    }
    component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
