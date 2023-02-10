#!/usr/bin/env python3
# encoding: utf-8
import json

from digitforce.aip.common.utils import component_helper
from digitforce.aip.common.utils.argument_helper import df_argument_helper
from crowd_expansion import crowd_expansion

def run():
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--seeds_crowd_table_name", type=str, required=False, help="种子人群")
    df_argument_helper.add_argument("--predict_crowd_table_name", type=str, required=False, help="待扩散人群")
    df_argument_helper.add_argument("--output_file_name", type=str, required=False, help="输出文件名称")

    user_vec_table_name = df_argument_helper.get_argument("user_vec_table_name")
    seeds_crowd_table_name = df_argument_helper.get_argument("seeds_crowd_table_name")
    predict_crowd_table_name = df_argument_helper.get_argument("predict_crowd_table_name")
    output_file_name = df_argument_helper.get_argument("output_file_name")
    seeds_crowd_table_name = json.loads(seeds_crowd_table_name).get('table_name')
    predict_crowd_table_name = json.loads(predict_crowd_table_name).get('table_name')
    print(f"user_vec_table_name:{user_vec_table_name}")
    print(f"seeds_crowd_table_name:{seeds_crowd_table_name}")
    print(f"predict_crowd_table_name:{predict_crowd_table_name}")
    print(f"output_file_name:{output_file_name}")
    crowd_expansion(user_vec_table_name=user_vec_table_name,
          seeds_crowd_table_name=seeds_crowd_table_name,
          predict_crowd_table_name=predict_crowd_table_name,
                    output_file_name=output_file_name
          )

    # component_helper.write_output("output_file_path", str(output_file_path))


if __name__ == '__main__':
    run()
