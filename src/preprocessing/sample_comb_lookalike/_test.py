#!/usr/bin/env python3
# encoding: utf-8

from sample_comb import sample_comb

sample_table_name = "algorithm.tmp_aip_sample"
sample_columns = ['user_id', 'item_id', 'label']
user_feature_table_name = "algorithm.tmp_aip_user_feature"
user_columns = ['user_id',
                'gender',
                'EDU',
                'RSK_ENDR_CPY',
                'NATN',
                'OCCU',
                'IS_VAIID_INVST',
                'u_buy_counts_30d',
                'u_amount_sum_30d',
                'u_amount_avg_30d',
                'u_amount_min_30d',
                'u_amount_max_30d',
                'u_buy_days_30d',
                'u_buy_avg_days_30d',
                'u_last_buy_days_30d',
                'u_buy_list']
item_feature_table_name = "algorithm.tmp_aip_item_feature"
item_columns = ['item_id',
                'fund_type',
                'management',
                'custodian',
                'invest_type',
                'i_buy_counts_30d',
                'i_amount_sum_30d',
                'i_amount_avg_30d',
                'i_amount_min_30d',
                'i_amount_max_30d']
train_data_table_name, test_data_table_name, user_data_table_name, hdfs_dir = sample_comb(sample_table_name,
                                                                                          sample_columns,
                                                                                          user_feature_table_name,
                                                                                          user_columns,
                                                                                          item_feature_table_name,
                                                                                          item_columns)
print(train_data_table_name, test_data_table_name, user_data_table_name, hdfs_dir)
