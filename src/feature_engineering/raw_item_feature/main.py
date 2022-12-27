#!/usr/bin/env python3
# encoding: utf-8

import json
import os

import digitforce.aip.common.utils.component_helper as component_helper
from calculate_raw_item_feature import calculate_raw_item_feature
from digitforce.aip.common.utils.argument_helper import df_argument_helper
from digitforce.aip.components.feature_engineering import RawItemFeatureOp


def run():
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=True, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=True, help="name")
    df_argument_helper.add_argument("--name", type=str, required=True, help="name")

    raw_item_feature_table_name = calculate_raw_item_feature()
    output = {
        'type': 'hive_table',
        'table_name': raw_item_feature_table_name,
        'column_list': []
    }
    component_helper.write_output(RawItemFeatureOp.OUTPUT_RAW_ITEM_FEATURE, output)


if __name__ == '__main__':
    run()
