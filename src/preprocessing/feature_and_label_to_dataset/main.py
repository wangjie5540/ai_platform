#!/usr/bin/env python3
# encoding: utf-8

import os

from digitforce.aip.common.utils import component_helper
from digitforce.aip.common.utils.argument_helper import df_argument_helper
from feature_and_label_to_dataset import feature_and_label_to_dataset


def run():
    # 参数解析
    # for test
    # import os
    # import json
    # os.environ["global_params"] = json.dumps(
    #     {"op_name": {
    #         "raw_sample_table_name": "algorithm.tmp_aip_sample",
    #         "model_sample_table_name": "algorithm.tmp_aip_model_sample",
    #     }})
    # os.environ["name"] = "op_name"

    # todo tmp
    os.environ["train_dataset_table_name"] = "algorithm.train_dataset_table_name"
    os.environ["test_dataset_table_name"] = "algorithm.test_dataset_table_name"
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--label_table_name", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--model_user_feature_table_name", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--model_item_feature_table_name", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--train_dataset_table_name", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--test_dataset_table_name", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--train_p", type=str, required=False, help="样本数据")

    label_table_name = df_argument_helper.get_argument("label_table_name")
    model_user_feature_table_name = df_argument_helper.get_argument("model_user_feature_table_name")
    model_item_feature_table_name = df_argument_helper.get_argument("model_item_feature_table_name")
    train_dataset_table_name = df_argument_helper.get_argument("train_dataset_table_name")
    test_dataset_table_name = df_argument_helper.get_argument("test_dataset_table_name")
    train_dataset_table_name, test_dataset_table_name = feature_and_label_to_dataset(label_table_name,
                                                                                     model_user_feature_table_name,
                                                                                     model_item_feature_table_name,
                                                                                     train_dataset_table_name,
                                                                                     test_dataset_table_name, 0.8)

    component_helper.write_output("train_dataset_table_name", train_dataset_table_name)
    component_helper.write_output("test_dataset_table_name", test_dataset_table_name)


if __name__ == '__main__':
    run()
