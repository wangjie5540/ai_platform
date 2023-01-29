# coding: utf-8
import read_cos
import argparse
import digitforce.aip.common.utils.component_helper as component_helper


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', type=str, required=True, help='目标文件地址')
    parser.add_argument('--columns', type=str, required=True, help='列名，使用逗号分隔')
    args = parser.parse_args()
    table_name, columns = read_cos.read_to_table(args.url, args.columns)
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns
    }
    component_helper.write_output(outputs, need_json_dump=True)


if __name__ == '__main__':
    run()
