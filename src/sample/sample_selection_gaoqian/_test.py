from src.sample.sample_selection_gaoqian.sample_select import sample_create
import digitforce.aip.common.utils.spark_helper as spark_helper

data_table_name = 'aip.read_table_4698160228885073921'
columns = ['custom_id', 'fund_code',  'trade_type', 'i_fund_type', 'dt']
# spark_client = spark_helper.SparkClient()
# columns_str = ','.join(columns)
# data = spark_client.get_session().sql(f'select {columns_str} from {data_table_name}')
# print(data.show(5))
event_code_list = ['fund_buy', 'fund_buy']
category_a, category_b = '股票型', '混合型'
train_period, predict_period = 30, 30
table, col = sample_create(data_table_name, columns, event_code_list, category_a, category_b, train_period, predict_period)
print(table, col)
