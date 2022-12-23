import argparse
import json
from sample_select import sample_create
import digitforce.aip.common.utils.component_helper as component_helper

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    parser.add_argument("--name", type=str, required=True, help="名称")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["sample-sample_selection_gaoqian"][args.name]
    event_table_name = 'algorithm.zq_fund_trade'
    event_columns = ['custom_id', 'trade_type', 'fund_code', 'dt']
    item_table_name = 'algorithm.zq_fund_basic'
    item_columns =['ts_code', 'fund_type']
    event_code_list = component_params["event_code_list"]
    category_a = component_params["category"]
    train_period = component_params["train_period"]
    predict_period = component_params["predict_period"]
    table_name, columns = sample_create(event_table_name, event_columns, item_table_name, item_columns, event_code_list, category_a, train_period, predict_period)
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns
    }
    component_helper.write_output("sample", outputs)


if __name__ == '__main__':
    run()
