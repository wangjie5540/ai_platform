from src.feature_engineering.feature_create_gaoqian.feature_create import feature_create

train_period = 7
predict_period = 7

sample_table_name = "algorithm.tmp_aip_sample_gaoqian"
train_table_name, test_table_name = feature_create(sample_table_name, train_period, predict_period)
print(train_table_name)
