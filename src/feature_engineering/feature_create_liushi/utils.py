#!/usr/bin/env python3
# encoding: utf-8
import datetime


def get_jy_feature(now: int, featurelist: list):
    """
    now: 当前日期
    featurelist: 逆序排列 [(交易日，交易笔数，交易额，股票交易笔数，股票交易额，基金交易笔数，基金交易额), (), ()...]
    return: 最近一次交易距今天数，最近一次交易额，最近3/7/15/30天：交易笔数，交易额，股票交易笔数，股票交易额，基金交易笔数，基金交易额
    return示例：(14, 2,
                  [[2, 3, 4, 5, 6, 7],
                   [2, 3, 4, 5, 6, 7],
                   [3, 5, 7, 9, 11, 13],
                   [3, 5, 7, 9, 11, 13]]
                )
    """
    last_day_count = 999
    last_rmb = 0

    last_n = [3, 7, 15, 30]
    last_n_feature = [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]]

    if not featurelist:
        return last_day_count, last_rmb, last_n_feature

    flag = True
    for day, jycount, rmb, gpcount, gprmb, jjcount, jjrmb in featurelist:
        if int(day) > now:
            continue
        cul_day = day_diff(day, now)
        if flag:
            last_day_count = cul_day
            last_rmb = rmb
            flag = False
        for index, termday in enumerate(last_n):
            if cul_day < termday:
                last_n_feature[index][0] += jycount
                last_n_feature[index][1] += rmb
                last_n_feature[index][2] += gpcount
                last_n_feature[index][3] += gprmb
                last_n_feature[index][4] += jjcount
                last_n_feature[index][5] += jjrmb
    return last_day_count, last_rmb, last_n_feature


def get_act_feature(now: int, act_days: list):
    """
    now: 当前日期
    act_days: 活跃的日期list
    return: 最近3/7/15/30天的活跃天数，示例：[2, 2, 3, 3]
    """
    last_n = [3, 7, 15, 30]
    res = [0, 0, 0, 0]
    if not act_days:
        return res
    for day in act_days:
        if int(day) > now:
            break
        cul_day = day_diff(day, now)
        for index, termday in enumerate(last_n):
            if cul_day < termday:
                res[index] += 1
    return res


def get_zj_feature(now: int, featurelist: list):
    """
    now: 当前日期
    featurelist: 逆序排列 [(资金变动日期，资金转出金额，资金转入金额，资金转出笔数，资金转入笔数), (), ()...]
    return: 最近一次资金变动距今天数，最近一次资金变动金额，最近3/7/15/30天：资金转出金额，资金转入金额，资金转出笔数，资金转入笔数
    return示例： (9, 6, [[0, 0, 0, 0], [0, 0, 0, 0], [4, 10, 6, 7], [4, 10, 6, 7]])
    """

    last_day_count = 999
    last_rmb = 0

    last_n = [3, 7, 15, 30]
    last_n_feature = [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]

    if not featurelist:
        return last_day_count, last_rmb, last_n_feature

    flag = True
    for day, zc, zr, zc_cnt, zr_cnt in featurelist:
        if int(day) > now:
            continue
        cul_day = day_diff(day, now)
        if flag:
            last_day_count = cul_day
            last_rmb = zr - zc
            flag = False
        for index, termday in enumerate(last_n):
            if cul_day < termday:
                last_n_feature[index][0] += zc
                last_n_feature[index][1] += zr
                last_n_feature[index][2] += zc_cnt
                last_n_feature[index][3] += zr_cnt
    return last_day_count, last_rmb, last_n_feature


def format_list(inp: list):
    """
    inp: 例如 [[1,2,3], [4,5,6]]
    return: [1,2,3,4,5,6]
    """
    res = []
    for term in inp:
        res += term
    return res



def day_diff(date1: int, date2: int):
    """
    计算两个日期相差多少天
    @param start: 20210820
    @param end: 20210827
    @return: 7
    """
    start = str(date1)
    end = str(date2)
    old = datetime.datetime(int(start[0:4]), int(start[4:6]), int(start[6:8]))
    now = datetime.datetime(int(end[0:4]), int(end[4:6]), int(end[6:8]))
    count = (now - old).days
    return count


def genDict(inp):
    """
    inp: 输入一个rdd，单列
    return: 1. 将生成的dict存储hdfs，返回地址；2. 返回这个dict
    """
    sig_list = inp.distinct().collect()
    res_dict = dict()
    for index, term in enumerate(sig_list):
        res_dict[term] = index
    return res_dict