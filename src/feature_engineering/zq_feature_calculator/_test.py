#!/usr/bin/env python3
# encoding: utf-8
'''
@file: _test.py
@time: 2023/2/24 18:05
@desc:
'''
from digitforce.aip.common.aip_feature.zq_feature import *
raw_user_feature_table_name = "algorithm.tmp_test_raw_user_feature"
raw_item_feature_table_name = "algorithm.tmp_test_raw_item_feature"

init_feature_encoder_factory(raw_user_feature_table_name, raw_item_feature_table_name)
show_all_encoder()