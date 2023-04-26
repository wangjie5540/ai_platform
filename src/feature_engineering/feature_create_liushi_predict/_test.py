#!/usr/bin/env python3
# encoding: utf-8
from feature_create import feature_create

predict_table_name = "aip.cos_4748519187864883201"
active_before_days = 3
active_after_days = 1

predict_feature_table_name = feature_create(predict_table_name,
                                            active_before_days, active_after_days,
                                            feature_days=30)
