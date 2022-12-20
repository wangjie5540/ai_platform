#!/usr/bin/env python3
# encoding: utf-8
'''
@file: _test.py
@time: 2022/12/7 17:49
@desc:
'''
from sample_select import start_sample_selection

data_table_name = 'aip.read_table_4698160228885073921'
columns = ['custom_id',
           'trade_date',
           'trade_type',
           'fund_code',
           'trade_money',
           'fund_shares',
           'fund_nav',
           'dt',
           'u_gender',
           'u_EDU',
           'u_RSK_ENDR_CPY',
           'u_NATN',
           'u_OCCU',
           'u_IS_VAIID_INVST',
           'i_fund_type',
           'i_management',
           'i_custodian',
           'i_invest_type']
event_code = {'buy': 'fund_buy'}
table, col = start_sample_selection(event_code, pos_sample_proportion=0.5,
                                    pos_sample_num=50000)
print(table, col)
