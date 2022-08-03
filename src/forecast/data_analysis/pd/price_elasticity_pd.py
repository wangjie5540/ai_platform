# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    价格弹性模型
"""
import numpy as np
import pandas as pd
from statsmodels.formula.api import ols


def get_price_elasticity(data, cols, p='price', pe='pe'):
    model = ols(cols, data=data).fit()
    res = model.params.to_dict()
    res['r2'] = model.rsquared
    res['adj_r2'] = model.rsquared_adj
    res['f_pvalue'] = model.f_pvalue
    res['t_pvalue'] = model.pvalues[p]
    param = pd.DataFrame([res])
    param.rename(columns={p: pe}, inplace=True)
    return param


def linear_model(data, x_factor, y='sales_qty', p='price'):
    cols = y + " ~ " + p
    if x_factor is not None:
        for fac in x_factor:
            cols = cols + "+" + fac
    return get_price_elasticity(data, cols)


def log_level_model(data, x_factor, y='sales_qty', p='price'):
    obj_col = 'log_' + y
    data[obj_col] = np.log(data[y])
    cols = obj_col + " ~ " + p
    if x_factor is not None:
        for fac in x_factor:
            cols = cols + "+" + fac
    return get_price_elasticity(data, cols)


def level_log_model(data, x_factor, y='sales_qty', p='price'):
    log_p = 'log_' + p
    data[log_p] = np.log(data[p])
    cols = y + " ~ " + log_p
    if x_factor is not None:
        for fac in x_factor:
            col = "log_" + fac
            data[col] = np.log(data[fac])
            cols = cols + "+" + col
    return get_price_elasticity(data, cols, p=log_p)


def log_log_model(data, x_factor, y='sales_qty', p='price'):
    log_y = 'log_' + y
    data[log_y] = np.log(data[y])
    log_p = 'log_' + p
    data[log_p] = np.log(data[p])
    data[log_y] = np.log(data[y])
    cols = log_y + " ~ " + log_p
    if x_factor is not None:
        for fac in x_factor:
            col = "log_" + fac
            data[col] = np.log(data[fac])
            cols = cols + "+" + col
    return get_price_elasticity(data, cols, p=log_p)


def compute_goods_price_elasticity(data, x_factor, y, p, method):
    if method == 'linear':
        return linear_model(data, x_factor, y=y, p=p)
    elif method == 'log_level':
        return log_level_model(data, x_factor, y=y, p=p)
    elif method == 'level_log':
        return level_log_model(data, x_factor, y=y, p=p)
    else:
        return log_log_model(data, x_factor, y=y, p=p)


def compute_price_elasticity(sales, x_factor=None, keys=['shop_id', 'goods_id'], y='sales_qty', p='price',
                             method='log_log'):
    res = sales.groupby(keys).apply(compute_goods_price_elasticity, x_factor, y, p, method).reset_index()
    new_cols = keys.copy()
    new_cols.append('pe')
    new_cols.append('r2')
    new_cols.append('adj_r2')
    new_cols.append('f_pvalue')
    new_cols.append('t_pvalue')
    return res[new_cols]


def transfer_model(pe, pe_sign, s):
    pe['s_pe'] = pe[pe_sign] * pe[s]
    return pe['s_pe'].sum() / pe[s].sum()


def compute_pe_by_transfer_model(sim, pe, pe_sign='pe', s='similarity', st=0.5, keys=None,
                                 sk=None, B_sign='_B'):
    if keys is None:
        keys = ['shop_id', 'goods_id']
    if sk is None:
        sk = ['goods_id']
    similarity = sim[sim[s] >= st]
    # pe_usable = pe[(pe[pe_sign] <= pt) & (pe[r] >= rt)]
    pe_usable = pe
    new_keys = []
    for key in sk:
        pe_usable.rename(columns={key: key + B_sign}, inplace=True)
        new_keys.append(key + B_sign)
    pe_usable = pe_usable.merge(similarity, on=new_keys, how='inner')
    pe_trans = pe_usable.groupby(keys).apply(transfer_model, pe_sign, s).reset_index(drop=True)
    return pe_trans


def cross_price_elasticity_model(transfer_model, pe_sign):
    pass
