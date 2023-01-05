#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
from feature_create import feature_create
import digitforce.aip.common.utils.component_helper as component_helper

from digitforce.aip.common.utils.argument_helper import df_argument_helper


def run():
    # 参数解析

    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="名称")
    df_argument_helper.add_argument("--sample", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--active_before_days", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--active_after_days", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--start_date", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--end_date", type=str, required=False, help="样本数据")

    active_before_days = int(df_argument_helper.get_argument("active_before_days"))
    active_after_days = int(df_argument_helper.get_argument("active_after_days"))

    start_date = df_argument_helper.get_argument("start_date")
    end_date = df_argument_helper.get_argument("end_date")
    sample_table_name = df_argument_helper.get_argument("sample")
    predict_feature_table_name = feature_create(sample_table_name,
                                                active_before_days, active_after_days,
                                                start_date, end_date,
                                                feature_days=30)

    component_helper.write_output("predict_feature_table_name", predict_feature_table_name)


if __name__ == '__main__':
    run()
