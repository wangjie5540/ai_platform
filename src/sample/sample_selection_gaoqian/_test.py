from src.sample.sample_selection_gaoqian.sample_select import sample_create

event_code = "shengou"
train_period = 7
predict_period = 7
category = "simu"

# todo: 类别中英文映射转换
category_map = {
    "gongmu": "公墓基金",
    "xintuo": "信托产品",
    "simu": "私募基金"
}
category = category_map.get(category)

event_code_map = {
    "shengou": "申购",
    "shuhui": "赎回",
    "rengou": "认购"
}
event_code = event_code_map.get(event_code)

# TODO 从参数中获取
trade_table_name = 'zq_standard.dm_cust_subs_redm_event_df'
trade_columns = ['cust_code', 'event_code', 'product_type_pri', 'dt']
event_table_name = 'zq_standard.dm_cust_traf_behv_aggregate_df'
event_columns = ['cust_code', 'is_login', 'dt']
table_name, columns = sample_create(trade_table_name, trade_columns, event_table_name, event_columns, event_code, category, train_period, predict_period)
print(table_name, columns)
