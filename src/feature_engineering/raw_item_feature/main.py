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
    os.environ["raw_item_feature_table_name"] = "algorithm.tmp_raw_item_feature_table_name"
    os.environ["global_params"] = json.dumps(
        {"container_name": {"raw_item_feature_table_name": "algorithm.tmp_raw_item_feature_table_name"}})
    os.environ["name"] = "container_name"
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--raw_item_feature_table_name",
                                    default="algorithm.tmp_raw_item_feature_table_name",
                                    type=str, required=False,
                                    help="raw_item_feature_table_name")

    raw_item_feature_table_name = df_argument_helper.get_argument("raw_item_feature_table_name")
    raw_item_feature_table_name = calculate_raw_item_feature(raw_item_feature_table_name)

    component_helper.write_output(RawItemFeatureOp.OUTPUT_KEY_RAW_ITEM_FEATURE, raw_item_feature_table_name)


if __name__ == '__main__':
    run()
