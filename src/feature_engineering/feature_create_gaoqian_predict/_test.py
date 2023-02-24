from src.feature_engineering.feature_create_gaoqian_predict.feature_create import feature_create

train_period = 30
predict_period = 30
sample_table_name = "algorithm.tmp_aip_sample_gaoqian"
print('sample_table_name', sample_table_name)
predict_table_name = feature_create(sample_table_name, train_period, predict_period)
print(predict_table_name)
