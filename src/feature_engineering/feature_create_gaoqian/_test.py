from src.feature_engineering.feature_create_gaoqian.feature_create import feature_create

event_table_name = 'algorithm.zq_fund_trade'
item_table_name = 'algorithm.zq_fund_basic'
user_table_name = 'algorithm.user_info'
event_columns = ['custom_id', 'trade_type', 'fund_code', 'trade_money', 'dt']
item_columns = ['ts_code', 'fund_type']
user_columns = ['CUST_ID', 'gender', 'EDU', 'RSK_ENDR_CPY', 'NATN', 'OCCU', 'IS_VAIID_INVST',]
event_code = 'fund_buy'
category = '股票型'
sample_table_name = "algorithm.tmp_aip_sample_gaoqian"

train_data_table_name, test_data_table_name, columns = feature_create(event_table_name, event_columns, item_table_name, item_columns, user_table_name, user_columns, event_code, category, sample_table_name)

print(train_data_table_name)
