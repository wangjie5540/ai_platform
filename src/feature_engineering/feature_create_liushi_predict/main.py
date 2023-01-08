#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
from feature_create import feature_create
import digitforce.aip.common.utils.component_helper as component_helper

from digitforce.aip.common.utils.argument_helper import df_argument_helper


def run():
    # 参数解析
#     import json
#     import os
#     global_params = json.dumps({
#   "model-predict": {
#     "predict_table_name": 3,
#     "model_hdfs_path": 5
#   },
#   "feature_create_predict": {
#     "active_before_days": 3,
#     "active_after_days": 5,
#     "start_date": "20221211",
#     "end_date": "20221220",
#     "sample": "algorithm.aip_zq_liushi_custom_predict"
#   }
# })
#     name = "feature_create_predict"
#     os.environ["global_params"] = global_params
#     os.environ["name"] = name
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="名称")
    df_argument_helper.add_argument("--sample", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--active_before_days", type=str, required=False, help="过去活跃天数")
    df_argument_helper.add_argument("--active_after_days", type=str, required=False, help="未来不活跃天数")
    print("===================================================================")
    print("run on dev env")
    print(f"global_params {df_argument_helper.get_argument('global_params')}")
    print(f"name {df_argument_helper.get_argument('name')}")
    print(f"active_before_days {df_argument_helper.get_argument('active_before_days')}")
    print(f"active_after_days {df_argument_helper.get_argument('active_after_days')}")
    print(f"sample {df_argument_helper.get_argument('sample')}")
    predict_feature_table_name = "algorithm.aip_zq_liushi_custom_feature_predict"
    print(f"predict_feature_table_name:{predict_feature_table_name}")
    print("===================================================================")
    component_helper.write_output("predict_feature_table_name", predict_feature_table_name)

    return
    active_before_days = int(df_argument_helper.get_argument("active_before_days"))
    active_after_days = int(df_argument_helper.get_argument("active_after_days"))

    sample_table_name = df_argument_helper.get_argument("sample")
    predict_feature_table_name = feature_create(sample_table_name,
                                                active_before_days, active_after_days,
                                                feature_days=30)

    component_helper.write_output("predict_feature_table_name", predict_feature_table_name)


if __name__ == '__main__':
    run()
