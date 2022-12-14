#!/usr/bin/env python3
# encoding: utf-8
'''
@file: _test.py
@time: 2022/12/9 18:32
@desc:
'''
from sample_comb import sample_comb

sample_table_name = "algorithm.tmp_aip_sample"
sample_columns = ['user_id', 'item_id', 'label']
user_feature_table_name = "algorithm.tmp_aip_user_feature"
user_columns = ['user_id',
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
item_feature_table_name = "algorithm.tmp_aip_item_feature"
item_columns = ['item_id',
                'i_buy_counts_30d',
                'i_amount_sum_30d',
                'i_amount_avg_30d',
                'i_amount_min_30d',
                'i_amount_max_30d',
                'i_fund_type',
                'i_management',
                'i_custodian',
                'i_invest_type']
train_data_table_name, test_data_table_name, user_data_table_name, hdfs_dir = sample_comb(sample_table_name,
                                                                                          sample_columns,
                                                                                          user_feature_table_name,
                                                                                          user_columns,
                                                                                          item_feature_table_name,
                                                                                          item_columns)
print(train_data_table_name, test_data_table_name, user_data_table_name, hdfs_dir)
