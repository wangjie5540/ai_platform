from feature_create import feature_create

train_period = 7
predict_period = 7
sample_table_name = "aip.cos_4740930808218390529"
print('sample_table_name', sample_table_name)
predict_table_name = feature_create(sample_table_name, train_period, predict_period)
print(predict_table_name)
