#!/usr/bin/env python3
# encoding: utf-8

import digitforce.aip.common.utils.component_helper as component_helper
from calculate_raw_user_feature import calculate_raw_user_feature
from digitforce.aip.common.utils.argument_helper import df_argument_helper


def run():
    # for test
    import os
    import json
    os.environ["global_params"] = json.dumps(
        {"raw_user_feature": {"raw_user_feature_table_name": "algorithm.tmp_raw_user_feature_table_name"}})
    os.environ["name"] = "raw_user_feature"
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--raw_user_feature_table_name", type=str, required=False, help="样本数据")

    raw_user_feature_table_name = df_argument_helper.get_argument("raw_user_feature_table_name")
    raw_user_feature_table_name = calculate_raw_user_feature(raw_user_feature_table_name)

    component_helper.write_output("raw_user_feature_table_name", raw_user_feature_table_name)


if __name__ == '__main__':
    run()
