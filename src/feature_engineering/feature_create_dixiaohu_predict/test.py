#!/usr/bin/env python3
# encoding: utf-8
from digitforce.aip.common.utils.spark_helper import SparkClient
from decimal import Decimal
from feature_create import *

spark_client = SparkClient.get()
spark = spark_client.get_session()


sample_table_name = "algorithm.aip_zq_dixiaohu_custom_label"
dixiao_before_days = 1
dixiao_after_days = 10
feature_days = 10
train_table_name, test_table_name = feature_create(
        sample_table_name,
        dixiao_before_days=dixiao_before_days,
        dixiao_after_days=dixiao_after_days,
        feature_days=feature_days,
    )