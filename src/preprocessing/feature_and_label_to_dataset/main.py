#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json

from digitforce.aip.common.utils import component_helper
from feature_and_label_to_dataset import feature_and_label_to_dataset


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=str, required=True, help="样本")
    parser.add_argument("--user_feature", type=str, required=True, help="用户特征")
    parser.add_argument("--item_feature", type=str, required=True, help="物品特征")
    args = parser.parse_args()
    sample = json.loads(args.sample)
    user_feature = json.loads(args.user_feature)
    item_feature = json.loads(args.item_feature)
    train_data_table_name, test_data_table_name, user_data_table_name, data_table_columns, user_data_table_columns, hdfs_dir = feature_and_label_to_dataset(
        sample['table_name'], sample['column_list'],
        user_feature['table_name'], user_feature['column_list'],
        item_feature['table_name'], item_feature['column_list'])

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
        "path": hdfs_dir
    }
    component_helper.write_output("train_data", train_data_table_output)
    component_helper.write_output("test_data", test_data_table_output)
    component_helper.write_output("user_data", user_data_table_output)
    component_helper.write_output("other_data", hdfs_output)


if __name__ == '__main__':
    run()
