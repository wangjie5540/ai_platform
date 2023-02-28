#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
import os

from digitforce.aip.common.utils.argument_helper import df_argument_helper
from sample_select import start_sample_selection
import digitforce.aip.common.utils.component_helper as component_helper


def run():
    # 参数解析
    os.environ["event_code_buy"] = "申购"
    os.environ["pos_sample_proportion"] = "0.5"
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--event_code_buy", type=str, required=False, help="")
    df_argument_helper.add_argument("--pos_sample_proportion", type=str, required=False, help="")

    event_code_buy = df_argument_helper.get_argument("event_code_buy")
    pos_sample_proportion = float(df_argument_helper.get_argument("pos_sample_proportion"))
    table_name, columns = start_sample_selection(event_code_buy, pos_sample_proportion, pos_sample_num=10000)

    component_helper.write_output("sample", table_name)


if __name__ == '__main__':
    run()
