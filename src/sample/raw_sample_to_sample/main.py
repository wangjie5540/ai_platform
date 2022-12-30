#!/usr/bin/env python3
# encoding: utf-8

import digitforce.aip.common.utils.component_helper as component_helper
from digitforce.aip.common.utils.argument_helper import df_argument_helper
from to_sample import raw_sample_to_sample


def run():
    # for test
    import os
    import json
    os.environ["global_params"] = json.dumps(
        {"op_name": {
            "raw_sample_table_name": "algorithm.tmp_aip_sample",
            "model_sample_table_name": "algorithm.tmp_aip_model_sample",
        }})
    os.environ["name"] = "op_name"
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--raw_sample_table_name", type=str, required=False, help="样本数据")
    df_argument_helper.add_argument("--model_sample_table_name", type=str, required=False, help="样本数据")

    raw_sample_table_name = df_argument_helper.get_argument("raw_sample_table_name")
    model_sample_table_name = df_argument_helper.get_argument("model_sample_table_name")
    model_sample_table_name = raw_sample_to_sample(raw_sample_table_name, model_sample_table_name)

    component_helper.write_output("model_sample_table_name", model_sample_table_name)


if __name__ == '__main__':
    run()
