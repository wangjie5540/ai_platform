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
    global_params = json.loads(args.global_params)
    component_params = global_params[args.name]
    event_code_buy = component_params["event_code_buy"]
    pos_sample_proportion = component_params["pos_sample_proportion"]
    table_name, columns = start_sample_selection(event_code_buy, pos_sample_proportion, pos_sample_num=10000)
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns
    }
    component_helper.write_output("sample", outputs)


if __name__ == '__main__':
    run()
