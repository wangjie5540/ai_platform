# coding: utf-8
import argparse
import digitforce.aip.common.utils.component_helper as component_helper
import json
import transformer


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument('--global_params', type=str, required=True, help='全局参数')
    parser.add_argument('--name', type=str, required=True, help='名称')
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params['source-read_table'][args.name]
    select_sql = component_params['select_sql']
    columns = component_params['columns']
    print("select_sql: ", select_sql)
    print("columns: ", columns)
    table_name = transformer.transform(table_name=table_name, )
    # 构造向下游组件的输出
    columns_list = columns.split(",")
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns_list
    }
    component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
