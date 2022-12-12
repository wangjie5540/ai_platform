#!/usr/bin/env python3
# encoding: utf-8
'''
@file: _test.py
@time: 2022/12/9 18:32
@desc:
'''
from src.preprocessing.sample_comb_lookalike.sample_comb import sample_comb

sample_table_name = "algorithm.tmp_aip_sample"
user_feature_table_name = "algorithm.tmp_aip_user_feature"
item_feature_table_name = "algorithm.tmp_aip_item_feature"
train_data_table_name, columns, encoder_hdfs_path = sample_comb(sample_table_name, user_feature_table_name,
                                                                item_feature_table_name)
print(train_data_table_name)
