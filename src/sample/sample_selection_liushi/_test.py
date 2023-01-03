#!/usr/bin/env python3
# encoding: utf-8
from sample_select import start_sample_selection

active_before_days = 3
active_after_days = 5
start_date = "20221211"
end_date = "20221220"

sample_table_name = start_sample_selection(active_before_days, active_after_days,
                                           start_date, end_date,
                                           label_count=10000)
