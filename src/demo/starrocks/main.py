# coding: utf-8
import read_starrocks
import argparse
import json
import digitforce.aip.common.utils.component_helper as component_helper


def run():
    # 初始化组件
    component_helper.init_config()
    # 解析全局参数
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, help='your component name')
    parser.add_argument('--global_params', type=str, help='your global params')
    args = parser.parse_args()
    global_params = json.loads(args.global_params)

    # 获取组件参数
    component_params = global_params[args.name]
    table_name = component_params['table_name']
    limit = component_params['table_name']
    read_starrocks.do_read(table_name=table_name, limit=limit)


if __name__ == '__main__':
    run()
