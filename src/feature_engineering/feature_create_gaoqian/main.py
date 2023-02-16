#!/usr/bin/env python3
# encoding: utf-8

import argparse
import json
from digitforce.aip.common.utils import component_helper
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
    event_code = component_params["event_code"]
    category = component_params['category']

    event_table_name = 'algorithm.zq_fund_trade'
    item_table_name = 'algorithm.zq_fund_basic'
    user_table_name = 'algorithm.user_info'
    event_columns = ['custom_id', 'trade_type', 'fund_code', 'trade_money', 'dt']
    item_columns = ['ts_code', 'fund_type']
    user_columns = ['CUST_ID', 'gender', 'EDU', 'RSK_ENDR_CPY', 'NATN', 'OCCU', 'IS_VAIID_INVST', ]

    sample_table_name = json.loads(args.sample)
    train_table_name, test_table_name, columns = feature_create(event_table_name, event_columns, item_table_name, item_columns, user_table_name, user_columns, event_code, category, sample_table_name)
    component_helper.write_output("train_feature_table_name", train_table_name)
    component_helper.write_output("test_feature_table_name", test_table_name)


if __name__ == '__main__':
    run()
