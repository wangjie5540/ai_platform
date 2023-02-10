from src.sample.sample_selection_gaoqian.sample_select import sample_create

event_table_name = 'algorithm.zq_fund_trade'
item_table_name = 'algorithm.zq_fund_basic'
event_columns = ['custom_id', 'trade_type', 'fund_code', 'dt']
item_columns = ['ts_code', 'fund_type']
event_code = 'fund_buy'
category_a = '股票型'
train_period, predict_period = 30, 30
table, col = sample_create(event_table_name, event_columns, item_table_name, item_columns, event_code, category_a, train_period, predict_period)
print(table, col)
