
# encoding: utf-8

import argparse
import json
import digitforce.aip.common.utils.component_helper as component_helper
component_helper.init_config()
from sample_select import start_sample_selection # NOQA: E402



def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=False, help="全局参数")
    parser.add_argument("--name", type=str, required=False, help="名称")
    args = parser.parse_args()

    global_params = args.global_params
    global_params = json.loads(global_params)

    dixiao_before_days = global_params[args.name]['dixiao_before_days']
    dixiao_after_days = global_params[args.name]['dixiao_after_days']
    right_zc_threshold = global_params[args.name]['right_zc_threshold']
    avg_zc_threshold = global_params[args.name]['avg_zc_threshold']
    event_tag = global_params[args.name]['event_tag']

    print(f"global_params:{global_params}")
    print(f"name:{args.name}")
    print(f"sample_table_name:{sample_table_name}")
    print(f"event_tag:{event_tag}")

    event_map = {
        0: "login",
        1: "exchange",
    }
    event_tag = event_map.get(event_tag)
    
    sample_table_name = start_sample_selection(
        dixiao_before_days=dixiao_before_days,
        dixiao_after_days=dixiao_after_days,
        right_zc_threshold=right_zc_threshold,
        avg_zc_threshold=avg_zc_threshold,
        event_tag=event_tag,
    )
    component_helper.write_output("sample_table_name", sample_table_name)


if __name__ == "__main__":
    run()
