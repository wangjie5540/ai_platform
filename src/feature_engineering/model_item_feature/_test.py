#!/usr/bin/env python3
# encoding: utf-8
'''
@file: _test.py
@time: 2023/2/24 11:19
@desc:
'''


from raw_item_feature_to_model_item_feature import raw_feature2model_feature

raw_user_feature_table_name = "algorithm.tmp_test_raw_user_feature"

model_user_feature_table_name = "tmp_model_user_feature_table_name"


raw_feature2model_feature(raw_user_feature_table_name, model_user_feature_table_name)
print(123)