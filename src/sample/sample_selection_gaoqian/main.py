import argparse
import json
from src.sample.sample_selection_gaoqian.sample_select import sample_create

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["sample.sample_selection_gaoqian"]
    event_table_name = component_params["event_table_name"]
    event_columns = component_params["event_columns"]
    item_table_name = component_params["item_table_name"]
    item_columns = component_params["item_columns"]
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
    component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
