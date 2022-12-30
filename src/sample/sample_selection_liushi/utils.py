#!/usr/bin/env python3
# encoding: utf-8
'''
@file: aa.py
@time: 2022/12/30 10:48
@desc:
'''
import datetime


def getActiveDays(inp_date: set, start_date: str, end_date: str, before_days: int, after_days: int):
    """
    inp_date: 输入某个客户的活跃天数，例如set("20220101", "20220103", "20220105")
    start_date: 需要计算样本起始时间
    end_date: 需要计算样本结束时间
    before_days: 过去n天
    after_days: 在未来m天

    return: 某个客户在start_date到end_date之间，每一天，过去n天的活跃天数，未来m天的活跃天数，例如：[('20220103', 1, 1), ('20220104', 0, 2)]
    """
    res = []
    # date类型
    # 比如当前是20200315，该值的取值范围是(start_date, end_date)
    # 过去5天：  (left_date1, right_date1), (20200311 - 20200315)
    # 未来10天： (left_date2, right_date2), (20200316 - 20200325)
    right_date1 = datetime.datetime.strptime(start_date, '%Y%m%d')
    left_date1 = right_date1 - datetime.timedelta(days=before_days - 1)
    right_date2 = right_date1 + datetime.timedelta(days=after_days)
    left_date2 = right_date1 + datetime.timedelta(days=1)

    left_days = 0
    right_days = 0

    # 初始化
    date_tmp = left_date1
    while int(date_tmp.strftime("%Y%m%d")) <= int(right_date1.strftime("%Y%m%d")):
        if date_tmp.strftime("%Y%m%d") in inp_date:
            left_days += 1
        date_tmp += datetime.timedelta(days=1)

    date_tmp = left_date2
    while int(date_tmp.strftime("%Y%m%d")) <= int(right_date2.strftime("%Y%m%d")):
        if date_tmp.strftime("%Y%m%d") in inp_date:
            right_days += 1
        date_tmp += datetime.timedelta(days=1)

    res.append((start_date, left_days, right_days))

    for i in range(
            (datetime.datetime.strptime(end_date, '%Y%m%d') - datetime.datetime.strptime(start_date, '%Y%m%d')).days):
        new_right_date1 = right_date1 + datetime.timedelta(days=1)
        new_left_date1 = left_date1 + datetime.timedelta(days=1)
        new_right_date2 = right_date2 + datetime.timedelta(days=1)
        new_left_date2 = left_date2 + datetime.timedelta(days=1)

        if left_date1.strftime("%Y%m%d") in inp_date:
            left_days -= 1
        if new_right_date1.strftime("%Y%m%d") in inp_date:
            left_days += 1
        if left_date2.strftime("%Y%m%d") in inp_date:
            right_days -= 1
        if new_right_date2.strftime("%Y%m%d") in inp_date:
            right_days += 1

        res.append((new_right_date1.strftime('%Y%m%d'), left_days, right_days))

        right_date1 = new_right_date1
        left_date1 = new_left_date1
        right_date2 = new_right_date2
        left_date2 = new_left_date2

    return res