#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json

from src.sample.sample_selection_lookalike.sample_select import start_sample_selection


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["sample.sample_selection_lookalike"]
    data_table_name = component_params["data_table_name"]
    columns = component_params["columns"]
    # TODO: 重复使用的参数如何放置？
    event_code = component_params["event_code"]
    pos_sample_proportion = component_params["pos_sample_proportion"]
    table_name, columns = start_sample_selection(data_table_name, columns, event_code, pos_sample_proportion)
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns
    }
    component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
