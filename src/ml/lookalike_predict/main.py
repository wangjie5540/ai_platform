#!/usr/bin/env python3
# encoding: utf-8

from digitforce.aip.common.utils import component_helper
from digitforce.aip.common.utils.argument_helper import df_argument_helper
from crowd_expansion import crowd_expansion



def run():
    # 参数解析
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="name")
    df_argument_helper.add_argument("--user_vec_table_name", type=str, required=False, help="用户向量表")
    df_argument_helper.add_argument("--seeds_crowd_table_name", type=str, required=False, help="种子人群")
    df_argument_helper.add_argument("--predict_crowd_table_name", type=str, required=False, help="待扩散人群")
    df_argument_helper.add_argument("--result_hdfs_path", type=str, required=False, help="hdfs上传结果路径")

    user_vec_table_name = df_argument_helper.get_argument("user_vec_table_name")
    seeds_crowd_table_name = df_argument_helper.get_argument("seeds_crowd_table_name")
    predict_crowd_table_name = df_argument_helper.get_argument("predict_crowd_table_name")
    result_hdfs_path = df_argument_helper.get_argument("result_hdfs_path")

    print(f"user_vec_table_name:{user_vec_table_name}")
    print(f"seeds_crowd_table_name:{seeds_crowd_table_name}")
    print(f"predict_crowd_table_name:{predict_crowd_table_name}")
    print(f"result_hdfs_path:{result_hdfs_path}")
    crowd_expansion(user_vec_table_name=user_vec_table_name,
          seeds_crowd_table_name=seeds_crowd_table_name,
          predict_crowd_table_name=predict_crowd_table_name,
                    result_hdfs_path=result_hdfs_path
          )

    component_helper.write_output("result_hdfs_path", str(result_hdfs_path))


if __name__ == '__main__':
    run()
