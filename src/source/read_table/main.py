# coding: utf-8
import argparse
from digitforce.aip.common.logging_config import setup_logging
import read_table
import digitforce.aip.common.utils.component_helper as component_helper
import json


def run():
    setup_logging("info.log", "error.log")
    parser = argparse.ArgumentParser()

    parser.add_argument('--source_input', type=str, required=True, help='数据源参数')
    args = parser.parse_args()
    source_desc = json.loads(args.source_input)
    output = read_table.read_table_to_hive(source_desc)
    component_helper.write_output(output)


if __name__ == '__main__':
    run()
