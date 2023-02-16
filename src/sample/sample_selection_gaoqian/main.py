import argparse
import json
import digitforce.aip.common.utils.component_helper as component_helper
component_helper.init_config()
from sample_select import sample_create
import digitforce.aip.common.utils.component_helper as component_helper
import os
from digitforce.aip.common.utils.argument_helper import df_argument_helper


def run():

    # Default
    os.environ["train_period"] = 30
    os.environ["predict_period"] = 30
    os.environ["category"] = "混合型"
    os.environ["event_code"] = "fund_buy"
    # Reading arguments with two methods
    df_argument_helper.add_argument("--global_params", type=str, required=False, help="全局参数")
    df_argument_helper.add_argument("--name", type=str, required=False, help="名称")
    df_argument_helper.add_argument("--event_code", type=str, required=False, help="事件对应的取值")
    df_argument_helper.add_argument("--train_period", type=int, requied=False, help="过去X天（选取数据时间范围）")
    df_argument_helper.add_argument("--predict_period", type=int, requied=False, help="未来Y天（未来观测期）")
    df_argument_helper.add_argument("--category", type=str, required=False, help="")

    # Temporary
    event_table_name = 'algorithm.zq_fund_trade'
    event_columns = ['custom_id', 'trade_type', 'fund_code', 'dt']
    item_table_name = 'algorithm.zq_fund_basic'
    item_columns =['ts_code', 'fund_type']

    event_code = df_argument_helper.get_argument("event_code")
    category = df_argument_helper.get_argument("category")
    train_period = df_argument_helper.get_argument("train_period")
    predict_period = df_argument_helper.get_argument("predict_period")
    table_name, columns = sample_create(event_table_name, event_columns, item_table_name, item_columns, event_code, category, train_period, predict_period)

    component_helper.write_output("sample", table_name)


if __name__ == '__main__':
    run()
