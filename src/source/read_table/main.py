# coding: utf-8
import argparse
import read_table
import digitforce.aip.common.utils.component_helper as component_helper
import json


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument('--global_params', type=str, required=True, help='全局参数')
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params['source.read_table']
    select_sql = component_params['select_sql']
    columns = component_params['columns']
    table_name = read_table.read_table_to_hive(select_sql)
    # 构造向下游组件的输出
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": [columns]
    }
    component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
