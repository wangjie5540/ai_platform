#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json

from digitforce.aip.common.utils import component_helper
from sample_comb import sample_comb


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_input1", type=str, required=True, help="上游组件传输参数")
    parser.add_argument("--data_input2", type=str, required=True, help="上游组件传输参数")
    parser.add_argument("--data_input3", type=str, required=True, help="上游组件传输参数")
    args = parser.parse_args()
    data_input1 = json.loads(args.data_input1)
    if data_input1["type"] == "hive_table":
        sample_table_name = data_input1["table_name"]
        sample_columns = data_input1["column_list"]
    else:
        raise ValueError("value error, the type must be hive table")
    data_input2 = json.loads(args.data_input2)
    if data_input2["type"] == "hive_table":
        user_feature_table_name = data_input2["table_name"]
        user_columns = data_input2["column_list"]
    else:
        raise ValueError("value error, the type must be hive table")
    data_input3 = json.loads(args.data_input3)
    if data_input2["type"] == "hive_table":
        item_feature_table_name = data_input3["table_name"]
        item_columns = data_input3["column_list"]
    else:
        raise ValueError("value error, the type must be hive table")
    train_data_table_name, test_data_table_name, user_data_table_name, data_table_columns, user_data_table_columns, hdfs_dir = sample_comb(
        sample_table_name,
        sample_columns,
        user_feature_table_name,
        user_columns,
        item_feature_table_name,
        item_columns)

    train_data_table_output = {
        "type": "hive_table",
        "table_name": train_data_table_name,
        "column_list": data_table_columns
    }
    test_data_table_output = {
        "type": "hive_table",
        "table_name": test_data_table_name,
        "column_list": data_table_columns
    }
    user_data_table_output = {
        "type": "hive_table",
        "table_name": user_data_table_name,
        "column_list": user_data_table_columns
    }
    hdfs_output = {
        "type": "hdfs",
        "pth": hdfs_dir
    }
    component_helper.write_output(train_data_table_output)
    component_helper.write_output(test_data_table_output)
    component_helper.write_output(user_data_table_output)
    component_helper.write_output(hdfs_output)


if __name__ == '__main__':
    run()
