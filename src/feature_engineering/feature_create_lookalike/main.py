#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json

from feature_create import feature_create
import digitforce.aip.common.utils.component_helper as component_helper


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="name")
    parser.add_argument("--sample", type=str, required=True, help="样本数据")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params[args.name]
    event_code = component_params["event_code_buy"]
    # 获取样本数据
    sample_data = json.loads(args.sample)
    if sample_data["type"] == "hive_table":
        sample_table_name = sample_data["table_name"]
        sample_column_list = sample_data["column_list"]
    else:
        raise Exception('sample data type error')
    user_feature_table_name, user_feature_table_columns, item_feature_table_name, item_feature_table_columns = \
        feature_create(event_code, sample_table_name, sample_column_list)

    output_user_feature_table = {
        "type": "hive_table",
        "table_name": user_feature_table_name,
        "column_list": user_feature_table_columns
    }
    output_item_feature_table = {
        "type": "hive_table",
        "table_name": item_feature_table_name,
        "column_list": item_feature_table_columns
    }
    component_helper.write_output("user_feature", output_user_feature_table)
    component_helper.write_output("item_feature", output_item_feature_table)


if __name__ == '__main__':
    run()
