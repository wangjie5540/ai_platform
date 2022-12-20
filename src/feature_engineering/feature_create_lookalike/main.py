#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json

from digitforce.aip.common.utils import component_helper
from feature_create import feature_create


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    parser.add_argument("--data_input", type=str, required=True, help="上游组件传输参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["feature_engineering-feature_create_lookalike"][args.name]
    # TODO：重复使用的参数如何放置？
    event_code = component_params["event_code"]
    data_input = json.loads(args.data_input)
    if data_input["type"] == "hive_table":
        sample_table_name = data_input["table_name"]
        sample_columns = data_input["column_list"]
    else:
        raise ValueError("value error, the type must be hive table")
    user_feature_table_name, user_feature_table_columns, item_feature_table_name, item_feature_table_columns = feature_create(
        event_code,
        sample_table_name, sample_columns)
    user_feature_table_output = {
        "type": "hive_table",
        "table_name": user_feature_table_name,
        "column_list": user_feature_table_columns
    }
    item_feature_table_output = {
        "type": "hive_table",
        "table_name": item_feature_table_name,
        "column_list": item_feature_table_columns
    }

    component_helper.write_output(user_feature_table_output)
    component_helper.write_output(item_feature_table_output)


if __name__ == '__main__':
    run()
