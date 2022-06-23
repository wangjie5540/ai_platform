# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    日期操作操作
"""
import datetime
from dateutil.relativedelta import relativedelta

def date_add_str(date_str,step_len,time_type='day'):
    """
    新的日期=当前日期+step_len*(day/周/月)
    :param date_str:当前日期
    :param step_len:多长时间
    :param time_type:day/week/month
    :return:新的日期
    """
    if time_type=='day':#日
        date_str=datetime.datetime.strptime(date_str,"%Y%m%d")
        date_str_add=date_str+datetime.timedelta(days=+step_len)
        date_str_add=str(date_str_add).replace('-','')[0:8]
    elif time_type=='week':#周
        date_str=datetime.datetime.strptime(date_str, "%Y%m%d")
        date_str_add=date_str + datetime.timedelta(weeks=+step_len)
        date_str_add=str(date_str_add).replace('-','')[0:8]
    else:#月
        date_str=datetime.datetime.strptime(date_str,'%Y%m%d')
        date_str_add=date_str+relativedelta(months=+step_len)
        date_str_add=str(date_str_add).replace('-','')[0:8]
    return date_str_add

def month_add_str(month_str,step_len):
    """
    月份加上len后的月份时间
    :param month_str: 月份，如：202205
    :param step_len: 长度
    :return: 加上len后的长度
    """
    if len(month_str)>6:
        month_str=month_str[0:6]
    elif len(month_str)<6:
        return None
    month_str=month_str+'01'
    month_str=date_add_str(month_str,step_len,time_type='month')
    month_str=month_str[0:6]
    return month_str