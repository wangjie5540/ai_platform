#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
from combination import sample_comb
from digitforce.aip.common.utils import component_helper

def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=str, required=True, help="样本")
    parser.add_argument("--user_feature", type=str, required=True, help="用户特征")

    args = parser.parse_args()
    sample = json.loads(args.sample)
    user_feature = json.loads(args.user_feature)

    sample_table_name = sample["table_name"]
    user_feature_table_name = user_feature["table_name"]
    sample_columns = sample["column_list"]
    user_columns = user_feature["column_list"]
    train_data_table_name, test_data_table_name, user_data_table_name, hdfs_dir, data_columns = sample_comb(sample_table_name,
                                                                                          sample_columns,
                                                                                          user_feature_table_name,
                                                                                          user_columns
                                                                                            )

    train_data_outputs = {
        "type": "hive_table",
        "table_name": train_data_table_name,
        "column_list": data_columns
    }
    test_data_outputs = {
        "type": "hive_table",
        "table_name": test_data_table_name,
        "column_list": data_columns
    }
    hdfs_outputs = {
        "type": "hdfs",
        "path": hdfs_dir
    }
    component_helper.write_output("train_data", train_data_outputs)
    component_helper.write_output("test_data", test_data_outputs)
    component_helper.write_output("other_data", hdfs_outputs)


if __name__ == '__main__':
    run()
