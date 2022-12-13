#!/usr/bin/env python3
# encoding: utf-8
'''
@file: _test.py
@time: 2022/12/9 18:06
@desc:
'''
from src.feature_engineering.feature_create_lookalike.feature_create import feature_create

data_table_name = "aip.read_table_4698160228885073921"
columns = ['custom_id', 'trade_date', 'trade_type', 'fund_code', 'trade_money', 'fund_shares', 'fund_nav', 'dt',
           'u_gender', 'u_EDU', 'u_RSK_ENDR_CPY', 'u_NATN', 'u_OCCU', 'u_IS_VAIID_INVST', 'i_fund_type', 'i_management',
           'i_custodian', 'i_invest_type']
event_code = {"buy": "fund_buy"}
sample_table_name = "algorithm.tmp_aip_sample"
user_feature_table_name, item_feature_table_name = feature_create(data_table_name, columns, event_code,
                                                                  sample_table_name)

print(user_feature_table_name, item_feature_table_name)
