#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
import digitforce.aip.common.utils.component_helper as component_helper
# 初始化组件
component_helper.init_config()
from feature_create import feature_create


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    parser.add_argument("--sample", type=str, required=True, help="样本数据")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params[args.name]
    active_before_days = component_params["active_before_days"]
    active_after_days = component_params["active_after_days"]
    sample_table_name = args.sample
    feature_days = 30
    # todo
    print("===================================================")
    # print("the dev model .... skip....")
    print(sample_table_name,
          active_before_days, active_after_days,
          feature_days)
    train_table_name = "algorithm.aip_zq_liushi_custom_feature_train"
    test_table_name = "algorithm.aip_zq_liushi_custom_feature_test"
    # component_helper.write_output("train_feature_table_name", train_table_name)
    # component_helper.write_output("test_feature_table_name", test_table_name)
    print("===================================================")
    # return
    train_table_name, test_table_name = feature_create(sample_table_name,
                                                       active_before_days, active_after_days,
                                                       feature_days=30)

    component_helper.write_output("train_feature_table_name", train_table_name)
    component_helper.write_output("test_feature_table_name", test_table_name)


if __name__ == '__main__':
    run()
