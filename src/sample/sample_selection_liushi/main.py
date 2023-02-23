#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
from sample_select import start_sample_selection
import digitforce.aip.common.utils.component_helper as component_helper


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    args = parser.parse_args()
    global_params = args.global_params
    global_params = json.loads(global_params)
    name = args.name
    component_params = global_params[args.name]
    active_before_days = component_params["active_before_days"]
    active_after_days = component_params["active_after_days"]
    active_days_threshold = component_params["active_days_threshold"]
    sample_table_name = "algorithm.aip_zq_liushi_custom_label"
    print("===============================================")
    # print("run on dev model....")
    print(f"global_params:{global_params}")
    print(f"name:{name}")
    print(f"sample_table_name:{sample_table_name}")
    print(f"active_before_days:{active_before_days}")
    print(f"active_after_days:{active_after_days}")
    component_helper.write_output("sample_table_name", sample_table_name)
    print("===============================================")
    # return
    sample_table_name = \
        start_sample_selection(active_before_days, active_after_days, active_days_threshold, label_count=300000)
    component_helper.write_output("sample_table_name", sample_table_name)


if __name__ == '__main__':
    run()
