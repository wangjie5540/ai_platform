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
    train_period = component_params["train_period"]
    predict_period = component_params['predict_period']

    sample_table_name = json.loads(args.sample).get('table_name')
    print('sample_table_name', sample_table_name)
    predict_table_name = feature_create(sample_table_name, train_period, predict_period)
    component_helper.write_output("predict_feature_table_name", predict_table_name)

if __name__ == '__main__':
    run()
