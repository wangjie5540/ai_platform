# encoding: utf-8
from feature_create import feature_create

sample_table_name = "algorithm.aip_zq_dixiaohu_custom_label"
dixiao_before_days = 1
dixiao_after_days = 1
feature_days = 1
train_table_name, test_table_name = feature_create(
    sample_table_name=sample_table_name,
    dixiao_before_days=dixiao_before_days,
    dixiao_after_days=dixiao_after_days,
    feature_days=feature_days,
)
