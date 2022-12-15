import argparse
import json
from src.sample.sample_selection_gaoqian.sample_select import sample_create

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=True, help="全局参数")
    args = parser.parse_args()
    global_params = json.loads(args.global_params)
    component_params = global_params["sample.sample_selection_gaoqian"]
    data_table_name = component_params["data_table_name"]
    columns = component_params["columns"]
    event_code_list = component_params["event_code_list"]
    category_a = component_params["category_a"]
    category_b = component_params["category_b"]
    train_period = component_params["train_period"]
    predict_period = component_params["predict_period"]
    table_name, columns = sample_create(data_table_name, columns, event_code_list, category_a, category_b, train_period, predict_period)
    outputs = {
        "type": "hive_table",
        "table_name": table_name,
        "column_list": columns
    }
    component_helper.write_output(outputs)


if __name__ == '__main__':
    run()
