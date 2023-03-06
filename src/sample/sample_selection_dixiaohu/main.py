
# encoding: utf-8

import argparse
import json
import digitforce.aip.common.utils.component_helper as component_helper
component_helper.init_config()
from digitforce.aip.common.utils.argument_helper import df_argument_helper # NOQA: E402
from sample_select import start_sample_selection # NOQA: E402



def run():
    # 参数解析
    df_argument_helper.add_argument(
        "--global_params", type=str, required=False, help="全局参数"
    )
    df_argument_helper.add_argument(
        "--name", type=str, required=False, help="名称")
    df_argument_helper.add_argument(
        "--dixiao_before_days", type=int, required=False, help="低效户前置时间"
    )
    df_argument_helper.add_argument(
        "--dixiao_after_days", type=int, required=False, help="低效户后置时间"
    )
    df_argument_helper.add_argument(
        "--right_zc_threshold", type=int, required=False, help="时点资产阈值"
    )
    df_argument_helper.add_argument(
        "--avg_zc_threshold", type=int, required=False, help="日均资产阈值"
    )
    df_argument_helper.add_argument(
        "--event_tag", type=int, required=False, help="事件标签"
    )

    dixiao_before_days = int(
        df_argument_helper.get_argument("dixiao_before_days"))
    dixiao_after_days = int(
        df_argument_helper.get_argument("dixiao_after_days"))
    right_zc_threshold = int(
        df_argument_helper.get_argument("right_zc_threshold"))
    avg_zc_threshold = int(df_argument_helper.get_argument("avg_zc_threshold"))
    event_tag = int(df_argument_helper.get_argument("event_tag"))
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
