import argparse
import json
import digitforce.aip.common.utils.component_helper as component_helper
component_helper.init_config()
from sample_select import sample_create
import digitforce.aip.common.utils.component_helper as component_helper


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--global_params", type=str, required=False, help="全局参数")
    parser.add_argument("--name", type=str, required=False, help="名称")
    args = parser.parse_args()

    global_params = json.loads(args.global_params)
    event_code = global_params[args.name]['event_code']
    train_period = global_params[args.name]['train_period']
    predict_period = global_params[args.name]['predict_period']
    category = global_params[args.name]['category']

    # todo: 类别中英文映射转换
    category_map = {
        "gongmu": "公墓基金",
        "xintuo": "信托产品"
    }
    category = category_map.get(category)

    event_code_map = {
        "shengou": "申购",
        "shuhui": "赎回",
        "rengou": "认购"
    }
    event_code = event_code_map.get(event_code)

    # TODO 从参数中获取
    trade_table_name = 'algorithm.dm_cust_subs_redm_event_df'
    trade_columns = ['cust_code', 'event_code', 'product_type_pri', 'dt']
    event_table_name = 'algorithm.dm_cust_traf_behv_aggregate_df'
    event_columns = ['cust_code', 'is_login', 'dt']
    table_name, columns = sample_create(trade_table_name, trade_columns, event_table_name, event_columns, event_code, category, train_period, predict_period)
    component_helper.write_output("sample", table_name)


if __name__ == '__main__':
    run()
