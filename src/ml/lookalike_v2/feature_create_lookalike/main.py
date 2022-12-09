#!/usr/bin/env python3
# encoding: utf-8
'''
@file: main.py
@time: 2022/12/7 18:54
@desc:
'''

import argparse
from src.ml.lookalike_v2.feature_create_lookalike.feature_create import feature_create
from digitforce.aip.common.argumen_helper import DigitforceAipCmdArgumentHelper

if __name__ == '__main__':
    helper = DigitforceAipCmdArgumentHelper()
    helper.add_argument('--solution_id', type=str, default='', help='solution id')
    helper.add_argument('--instance_id', type=str, default='', help='instance id')
    helper.add_argument('--data_table_name', type=str, default='', help='data table name')
    helper.add_argument('--columns', type=str, default='', help='data table columns')
    helper.add_argument('--event_code', type=str, default='', help='data event type mapping')
    solution_id = helper.get_argument("solution_id")
    instance_id = helper.get_argument("instance_id")
    data_table_name = helper.get_argument("data_table_name")
    columns = helper.get_argument("columns")
    event_code = helper.get_argument("event_code")
    table_name, columns = feature_create(data_table_name, columns, event_code)
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns
    }
    print(outputs)
    # components.out(outputs)