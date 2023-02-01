# coding: utf-8
import read_cos
import argparse
import digitforce.aip.common.utils.component_helper as component_helper
import json


def run():
    component_helper.init_config()
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, required=True, help='名称')
    parser.add_argument('--global_params', type=str, required=True, help='pipeline全局参数')
    parser.add_argument('--columns', type=str, required=True, help='列名，使用逗号分隔')
    args = parser.parse_args()
    url = json.loads(args.global_params)[args.name]['url']
    table_name, columns = read_cos.read_to_table(url, args.columns)
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns
    }
    component_helper.write_output('table_name', outputs, need_json_dump=True)


if __name__ == '__main__':
    run()
