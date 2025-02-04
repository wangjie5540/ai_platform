#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
import digitforce.aip.common.utils.component_helper as component_helper
# 初始化组件
component_helper.init_config()
from digitforce.aip.common.utils.argument_helper import df_argument_helper
from feature_create import feature_create


def run():
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="名称")
    df_argument_helper.add_argument("--sample", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--active_before_days", type=str, required=False, help="过去活跃天数")
    df_argument_helper.add_argument("--active_after_days", type=str, required=False, help="未来不活跃天数")
    print("===================================================================")
    print(f"global_params {df_argument_helper.get_argument('global_params')}")
    print(f"name {df_argument_helper.get_argument('name')}")
    print(f"active_before_days {df_argument_helper.get_argument('active_before_days')}")
    print(f"active_after_days {df_argument_helper.get_argument('active_after_days')}")
    print(f"sample {df_argument_helper.get_argument('sample')}")
    predict_feature_table_name = "algorithm.aip_zq_liushi_custom_feature_predict"
    print(f"predict_feature_table_name:{predict_feature_table_name}")
    print("===================================================================")
    active_before_days = int(df_argument_helper.get_argument("active_before_days"))
    active_after_days = int(df_argument_helper.get_argument("active_after_days"))

    sample_table_name = df_argument_helper.get_argument("sample")
    sample_table_name = json.loads(sample_table_name).get('table_name')
    predict_feature_table_name = feature_create(sample_table_name,
                                                active_before_days, active_after_days,
                                                feature_days=30)

    component_helper.write_output("predict_feature_table_name", predict_feature_table_name)


if __name__ == '__main__':
    run()
