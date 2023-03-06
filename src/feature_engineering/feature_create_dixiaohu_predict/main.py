
# encoding: utf-8

import argparse
import json
from feature_create import feature_create
import digitforce.aip.common.utils.component_helper as component_helper
from digitforce.aip.common.utils.argument_helper import df_argument_helper
component_helper.init_config()


def run():
    # 参数解析
    df_argument_helper.add_argument(
        "--global_params", type=str, required=False, help="全局参数"
    )
    df_argument_helper.add_argument(
        "--name", type=str, required=False, help="名称")
    df_argument_helper.add_argument(
        "--sample_table_name", type=str, required=False, help="样本数据"
    )
    df_argument_helper.add_argument(
        "--dixiao_before_days", type=int, required=False, help="低效户前置时间"
    )
    df_argument_helper.add_argument(
        "--dixiao_after_days", type=int, required=False, help="低效户后置时间"
    )
    df_argument_helper.add_argument(
        "--feature_days", type=int, required=False, help="特征时间"
    )

    sample_table_name = df_argument_helper.get_argument("sample_table_name")
    dixiao_before_days = int(
        df_argument_helper.get_argument("dixiao_before_days"))
    dixiao_after_days = int(
        df_argument_helper.get_argument("dixiao_after_days"))
    feature_days = int(df_argument_helper.get_argument("feature_days"))
    predict_table_name = feature_create(
        sample_table_name,
        dixiao_before_days=dixiao_before_days,
        dixiao_after_days=dixiao_after_days,
        feature_days=feature_days,
    )

    component_helper.write_output(
        "predict_feature_table_name", predict_table_name)


if __name__ == "__main__":
    run()
