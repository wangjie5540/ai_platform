#!/usr/bin/env python3
# encoding: utf-8

import digitforce.aip.common.utils.component_helper as component_helper
from digitforce.aip.common.utils.argument_helper import df_argument_helper
from raw_user_feature_to_model_user_feature import \
    raw_feature2model_feature


def run():
    # for test
    # import os
    # import json
    # os.environ["global_params"] = json.dumps(
    #     {"op_name": {"raw_user_feature_table_name": "algorithm.tmp_raw_user_feature_table_name",
    #     "model_user_feature_table_name": "algorithm.tmp_model_user_feature_table_name"}})
    # os.environ["name"] = "op_name"
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--raw_user_feature_table_name", type=str, required=False, help="原始特征")
    df_argument_helper.add_argument("--model_user_feature_table_name", type=str, required=False, help="模型的特征")

    # todo model_user_feature_table_name 的key 从组件中获取
    raw_user_feature_table_name = df_argument_helper.get_argument("raw_user_feature_table_name")
    model_user_feature_table_name = df_argument_helper.get_argument("model_user_feature_table_name")
    raw_feature2model_feature(raw_user_feature_table_name, model_user_feature_table_name)

    component_helper.write_output("model_user_feature_table_name", model_user_feature_table_name)


if __name__ == '__main__':
    run()
