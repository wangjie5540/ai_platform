# coding: utf-8
import argparse
import digitforce.aip.common.utils.component_helper as component_helper
import json
import read_cos


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, required=True, help='名称')
    parser.add_argument('--global_params', type=str, required=True, help='pipeline全局参数')
    args = parser.parse_args()
    x_tenant = json.loads(args.global_params).get('X_TENANT', None)
    url = json.loads(args.global_params)[args.name]['url']
    columns = json.loads(args.global_params)[args.name]['columns']
    table_name, columns = read_cos.read_to_table(url, columns, x_tenant)
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns
    }
    component_helper.write_output('table_name', outputs, need_json_dump=True)


if __name__ == '__main__':
    run()
