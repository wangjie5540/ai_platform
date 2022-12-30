#!/usr/bin/env python3
# encoding: utf-8
from feature_create import feature_create

sample_table_name = "algorithm.aip_zq_liushi_custom_label"
active_before_days = 1
active_after_days = 3
start_date = "20221201"
mid_date = "20221218"
end_date = "20221220"

train_data_table_name, test_data_table_name = feature_create(sample_table_name,
                                                             active_before_days, active_after_days,
                                                             start_date, mid_date, end_date,
                                                             feature_days=30)
