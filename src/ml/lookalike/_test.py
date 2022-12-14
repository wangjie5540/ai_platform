#!/usr/bin/env python3
# encoding: utf-8
from lookalike_model_train import start_model_train

train_data_table_name = "algorithm.tmp_aip_train_data"
test_data_table_name = "algorithm.tmp_aip_test_data"
user_data_table_name = "algorithm.tmp_aip_user_data"
hdfs_path = "/data/pycharm_project_950/src/preprocessing/sample_comb_lookalike/dir/"
train_data_columns = ['user_id',
                      'item_id',
                      'label',
                      'i_buy_counts_30d',
                      'i_amount_sum_30d',
                      'i_amount_avg_30d',
                      'i_amount_min_30d',
                      'i_amount_max_30d',
                      'i_fund_type',
                      'i_management',
                      'i_custodian',
                      'i_invest_type',
                      'u_buy_counts_30d',
                      'u_amount_sum_30d',
                      'u_amount_avg_30d',
                      'u_amount_min_30d',
                      'u_amount_max_30d',
                      'u_buy_days_30d',
                      'u_buy_avg_days_30d',
                      'u_last_buy_days_30d',
                      'u_buy_list',
                      'u_gender',
                      'u_EDU',
                      'u_RSK_ENDR_CPY',
                      'u_NATN',
                      'u_OCCU',
                      'u_IS_VAIID_INVST']
user_data_columns = ['user_id',
                     'u_buy_counts_30d',
                     'u_amount_sum_30d',
                     'u_amount_avg_30d',
                     'u_amount_min_30d',
                     'u_amount_max_30d',
                     'u_buy_days_30d',
                     'u_buy_avg_days_30d',
                     'u_last_buy_days_30d',
                     'u_buy_list',
                     'u_gender',
                     'u_EDU',
                     'u_RSK_ENDR_CPY',
                     'u_NATN',
                     'u_OCCU',
                     'u_IS_VAIID_INVST']
dnn_hidden_units = (256, 128, 64)
dnn_dropout = 0.2
batch_size = 256
lr = 0.01
start_model_train(train_data_table_name, test_data_table_name, user_data_table_name, hdfs_path,
                  train_data_columns, user_data_columns,
                  dnn_hidden_units=dnn_hidden_units, dnn_dropout=dnn_dropout,
                  batch_size=batch_size, lr=lr
                  )
