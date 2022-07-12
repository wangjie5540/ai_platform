# -*- coding: utf-8 -*-
# @Time : 2022/07/12
# @Author : Hunter


def category_similarity(a, b, cols):
    for col in cols:
        if a[col] == b.iloc[0][col]:
            return cols[col]
    return 0


def comp_goods_category_similarity(sku, goods, cols, s):
    res = goods.copy(deep=True)
    res[s] = res.apply(lambda x: category_similarity(x, sku, cols), axis=1)
    return res


def comp_category_similarity(goods_A, goods_B, keys=['shop_id', 'goods_id'], B_sign='_B', catg_cols=None, s='similarity'):
    res_col = keys.copy()
    cols = catg_cols
    if cols is None:
        cols = {'catg_s_id': 0.9, 'catg_m_id': 0.7, 'catg_l_id': 0.5}
    for key in keys:
        goods_B.rename(columns={key: key+B_sign}, inplace=True)
        res_col.append(key+B_sign)
    res_col.append(s)
    res = goods_A.groupby(keys).apply(comp_goods_category_similarity, goods_B, cols, s).reset_index()
    print(res.head())
    return res[res_col]
