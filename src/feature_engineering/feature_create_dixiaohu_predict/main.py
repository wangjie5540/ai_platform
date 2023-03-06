# encoding: utf-8

import argparse
import json
import digitforce.aip.common.utils.component_helper as component_helper # NOQA: E402
component_helper.init_config()
from feature_create import feature_create # NOQA: E402


def run():
    # 参数解析
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    parser.add_argument("--sample", type=str, required=True, help="样本数据")
    args = parser.parse_args()

    global_params = args.global_params
    global_params = json.loads(global_params)


    sample_table_name = args.sample
    dixiao_before_days = global_params[args.name]['dixiao_before_days']
    dixiao_after_days = global_params[args.name]['dixiao_after_days']
    feature_days = global_params[args.name]['feature_days']

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
