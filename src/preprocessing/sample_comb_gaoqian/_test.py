#!/usr/bin/env python3
# encoding: utf-8

from combination import sample_comb

sample_table_name = "algorithm.tmp_aip_sample_gaoqian"
sample_columns = ['user_id', 'label']
user_feature_table_name = "algorithm.tmp_aip_user_feature_gaoqian"
user_columns = ['user_id',
                'u_event1_counts_30d',
                'u_event1_amount_sum_30d',
                'u_event1_amount_avg_30d',
                'u_event1_amount_min_30d',
                'u_event1_amount_max_30d',
                'u_event1_days_30d',
                'u_event1_avg_days_30d',
                'u_last_event1_days_30d',
                'gender', 'EDU', 'RSK_ENDR_CPY', 'NATN', 'OCCU', 'IS_VAIID_INVST']

train_data_table_name, test_data_table_name, user_data_table_name, hdfs_dir = sample_comb(sample_table_name,
                                                                                          sample_columns,
                                                                                          user_feature_table_name,
                                                                                          user_columns
                                                                                          )
print(train_data_table_name, test_data_table_name, user_data_table_name, hdfs_dir)
