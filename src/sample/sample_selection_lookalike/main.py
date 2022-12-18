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
    parser.add_argument("--data_input", type=str, required=True, help="上个组件传递过来的参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    data_input = json.loads(args.data_input)
    component_params = global_params["sample.sample_selection_lookalike"]
    if data_input["type"] == "hive_table":
        table_name = data_input["table_name"]
        column_list = data_input["column_list"]
    else:
        raise Exception('data_input type error')
    # TODO: 重复使用的参数如何放置？
    event_code = component_params["event_code"]
    pos_sample_proportion = component_params["pos_sample_proportion"]
    table_name, columns = start_sample_selection(table_name, column_list, event_code, pos_sample_proportion)
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns
    }
    component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
