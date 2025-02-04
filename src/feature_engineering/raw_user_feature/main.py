#!/usr/bin/env python3
# encoding: utf-8

import digitforce.aip.common.utils.component_helper as component_helper

# 初始化组件
component_helper.init_config()
from calculate_raw_user_feature import calculate_raw_user_feature
from digitforce.aip.common.utils.argument_helper import df_argument_helper


def run():
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--raw_user_feature_table_name",
                                    default="algorithm.tmp_test_raw_user_feature",
                                    type=str, required=False, help="样本数据")

    raw_user_feature_table_name = df_argument_helper.get_argument("raw_user_feature_table_name")
    print(f"raw_user_feature_table_name:{raw_user_feature_table_name}")
    raw_user_feature_table_name = calculate_raw_user_feature(raw_user_feature_table_name)

    component_helper.write_output("raw_user_feature", raw_user_feature_table_name)


if __name__ == '__main__':
    run()
