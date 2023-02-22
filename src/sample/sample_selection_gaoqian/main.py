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
        "hunhe": "混合型",
        "gupiao": "股票型"
    }
    category = category_map.get(category)
    # TODO 从参数中获取
    event_table_name = 'algorithm.zq_fund_trade_lite'
    event_columns = ['custom_id', 'trade_type', 'fund_code', 'dt']
    item_table_name = 'algorithm.zq_fund_basic'
    item_columns =['ts_code', 'fund_type']
    table_name, columns = sample_create(event_table_name, event_columns, item_table_name, item_columns, event_code, category, train_period, predict_period)
    component_helper.write_output("sample", table_name)


if __name__ == '__main__':
    run()
