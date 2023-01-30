#!/usr/bin/env python3
# encoding: utf-8
from feature_create import feature_create

sample_table_name = "algorithm.aip_zq_liushi_custom_label"
active_before_days = 3
active_after_days = 5

train_data_table_name, test_data_table_name = feature_create(sample_table_name,
                                                             active_before_days, active_after_days,
                                                             feature_days=30)
print(train_data_table_name, test_data_table_name)
