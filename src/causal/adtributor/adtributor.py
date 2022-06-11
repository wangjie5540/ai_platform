# encoding: utf-8

import pandas as pd
import numpy as np
from digitforce.aip.common.logging_config import setup_console_log
import logging

def adtributor(control_data_file, experimental_data_file, feature_list, target_feature, target_is_rate, output1_file, output2_file):
    setup_console_log()
    logging.info("读取数据")
    control_data = pd.read_csv(control_data_file, index_col=0)
    experimental_data = pd.read_csv(experimental_data_file, index_col=0)
    logging.info("数据处理")
    data = data_processing(control_data, experimental_data, feature_list, target_feature, target_is_rate)
    logging.info("根因分析启动")
    if target_is_rate:
        Candidate, ExplanatorySet, dta = rate_root_cause_analysis(data)
    else:
        Candidate, ExplanatorySet, dta = root_cause_analysis(data)
    ExplanatorySet = transform_indicator(ExplanatorySet)
    logging.info("返回结果")
    pd.DataFrame(Candidate, columns=['维度', '惊喜度']).to_csv(output1_file, index=False)
    pd.DataFrame(ExplanatorySet).to_csv(output2_file, index=False)

def data_processing(control_data, experimental_data, feature_list, target_feature, target_is_rate):
    '''
    根据目标特征的类型进行数据处理
    '''
    if not target_is_rate:
        target_feature = target_feature[0]
        data_pre = pd.DataFrame(columns=['dimension', 'indicator', target_feature])
        for i in feature_list:
            dimension_agg = agg_data(control_data, i, target_feature)
            data_pre = pd.concat([data_pre, dimension_agg], axis=0)
        data_pre = data_pre.rename(columns={target_feature: 'pred'})

        data_act = pd.DataFrame(columns=['dimension', 'indicator', target_feature])
        for i in feature_list:
            dimension_agg = agg_data(experimental_data, i, target_feature)
            data_act = pd.concat([data_act, dimension_agg], axis=0)
        data_act = data_act.rename(columns={target_feature: 'actual'})
        data = pd.merge(data_pre, data_act, how='inner', on=['dimension', 'indicator'])
    else:
        target_numerator = target_feature[0]
        data_pre1 = pd.DataFrame(columns=['dimension', 'indicator', target_numerator])
        for i in feature_list:
            dimension_agg = agg_data(control_data, i, target_numerator)
            data_pre1 = pd.concat([data_pre1, dimension_agg], axis=0)
        data_pre1 = data_pre1.rename(columns={target_numerator: 'pred_1'})
        data_act1 = pd.DataFrame(columns=['dimension', 'indicator', target_numerator])
        for i in feature_list:
            dimension_agg = agg_data(experimental_data, i, target_numerator)
            data_act1 = pd.concat([data_act1, dimension_agg], axis=0)
        data_act1 = data_act1.rename(columns={target_numerator: 'actual_1'})
        data1 = pd.merge(data_pre1, data_act1, how='inner', on=['dimension', 'indicator'])
        target_denominator = target_feature[1]
        data_pre2 = pd.DataFrame(columns=['dimension', 'indicator', target_denominator])
        for i in feature_list:
            dimension_agg = agg_data(control_data, i, target_denominator)
            data_pre2 = pd.concat([data_pre2, dimension_agg], axis=0)
        data_pre2 = data_pre2.rename(columns={target_denominator: 'pred_2'})
        data_act2 = pd.DataFrame(columns=['dimension', 'indicator', target_denominator])
        for i in feature_list:
            dimension_agg = agg_data(experimental_data, i, target_denominator)
            data_act2 = pd.concat([data_act2, dimension_agg], axis=0)
        data_act2 = data_act2.rename(columns={target_denominator: 'actual_2'})
        data2 = pd.merge(data_pre2, data_act2, how='inner', on=['dimension', 'indicator'])
        data = pd.merge(data1, data2, how='inner', on=['dimension', 'indicator'])
    data.loc[:, ('dimension')] = data.loc[:, ('dimension')].astype(str)
    data.loc[:, ('indicator')] = data.loc[:, ('indicator')].astype(str)
    return data

def agg_data(data, groupby_feature, target_feature):
    '''
    将数据按指定维度分组，进行相加聚合
    :param data: 数据
    :param groupby_feature: 分组维度
    :param target_feature: 目标维度
    :return:
    '''
    dimension_agg = data.groupby(groupby_feature)[target_feature].sum()
    dimension_agg.index.name='indicator'
    dimension_agg = pd.DataFrame(dimension_agg).reset_index()
    dimension_agg['dimension'] = groupby_feature
    dimension_agg = dimension_agg[['dimension', 'indicator', target_feature]]
    return dimension_agg

def JS_divergence(p, q):
    '''
    计算JS散度
    '''
    if p == 0 and q == 0:
        return 0.0
    elif p == 0:
        return round((0.5 * q * np.log(2)), 10)
    elif q == 0:
        return round((0.5 * p * np.log(2)), 10)
    p = np.array(p)
    q = np.array(q)
    M = (p + q) / 2

    js1 = 0.5 * np.sum(p * np.log(p / M)) + 0.5 * np.sum(q * np.log(q / M))

    return round(float(js1), 10)

def transform_indicator(data):
    '''
    将ExplanatorySet分解为df，列分别为维度、元素、惊喜度
    '''
    df = pd.DataFrame(data)
    df['维度'] = df[0].map(lambda x:x.split('-',1)[0])
    df['元素'] = df[0].map(lambda x:x.split('-',1)[1])
    df['惊喜度'] = df[1]
    df.drop(columns=0,inplace=True)
    df.drop(columns=1,inplace=True)
    return df

def root_cause_analysis(df):
    '''
    adtributor根因分析，返回维度集合与细分元素集合，按惊喜度降序排列
    '''
    group = df.groupby('dimension').sum()
    group = group.reset_index()
    group = group.rename(columns={'pred': 'pred_sum', 'actual': 'actual_sum'})
    dta = pd.merge(df, group, on='dimension', how='left')

    dta['actual_sum'] = dta['actual_sum'].apply(lambda x: round(x))
    dta['pred_sum'] = dta['pred_sum'].apply(lambda x: round(x))

    dta['p'] = dta['pred'] / dta['pred_sum']
    dta['q'] = dta['actual'] / dta['actual_sum']

    # 第一步：计算JS散度——surprise 惊奇度
    dta['JS'] = dta[['p', 'q']].apply(lambda x: JS_divergence(x['p'], x['q']), axis=1)

    # 第二步：计算某维度下某因素波动占总体波动的比率----Explanatory power
    dta['EP'] = (dta['actual'] - dta['pred']) / (dta['actual_sum'] - dta['pred_sum'])
    dta.sort_values(['dimension', 'JS'], inplace=True, ascending=False)

    Teep = 0.01
    Tep = 0.9

    # Adtributor
    ExplanatorySet = {}
    Candidate = {}
    for i in dta['dimension'].unique():
        indicator = []
        EP = 0
        number = 0
        total = int(len(dta[dta['dimension'] == i]) * 0.2)
        for j in dta[dta['dimension'] == i]['indicator']:
            EP_ij = dta.loc[(dta['dimension'] == i) & (dta['indicator'] == j), 'EP'].values[0]
            if EP_ij > Teep:
                indicator.append(j)
                EP += EP_ij
            if number > total and EP > Tep:
                S = 0
                for k in indicator:
                    S_indicator = dta.loc[(dta['dimension'] == i) & (dta['indicator'] == k), 'JS'].values[0]
                    S += S_indicator
                    ExplanatorySet[str(i) + '-' + str(k)] = S_indicator
                Candidate[i] = S
                break
            number += 1
    Candidate_sorted_values = sorted(Candidate.items(), key=lambda x: x[1], reverse=True)
    ExplanatorySet_sorted_values = sorted(ExplanatorySet.items(), key=lambda x: x[1], reverse=True)
    return Candidate_sorted_values, ExplanatorySet_sorted_values, dta


# 根因分析
def rate_root_cause_analysis(df):
    # 第一步：计算某维度下某因素的真实值和预测值占总收入的差异性----Surprise
    group = df.groupby('dimension').sum()
    group = group.reset_index()
    group = group.rename(columns={'pred_1': 'pred_sum_1', 'actual_1': 'actual_sum_1', 'pred_2': 'pred_sum_2',
                                  'actual_2': 'actual_sum_2'})
    dta = pd.merge(df, group, on='dimension', how='left')

    dta['actual_sum_1'] = dta['actual_sum_1'].apply(lambda x: round(x))
    dta['pred_sum_1'] = dta['pred_sum_1'].apply(lambda x: round(x))

    dta['actual_sum_2'] = dta['actual_sum_2'].apply(lambda x: round(x))
    dta['pred_sum_2'] = dta['pred_sum_2'].apply(lambda x: round(x))

    dta['p_1'] = dta['pred_1'] / dta['pred_sum_1']
    dta['q_1'] = dta['actual_1'] / dta['actual_sum_1']

    dta['p_2'] = dta['pred_2'] / dta['pred_sum_2']
    dta['q_2'] = dta['actual_2'] / dta['actual_sum_2']

    # 第一步：计算JS散度——surprise 惊奇度
    dta['JS_1'] = dta[['p_1', 'q_1']].apply(lambda x: JS_divergence(x['p_1'], x['q_1']), axis=1)

    dta['JS_2'] = dta[['p_2', 'q_2']].apply(lambda x: JS_divergence(x['p_2'], x['q_2']), axis=1)

    dta['JS'] = dta['JS_1'] + dta['JS_2']

    dta.sort_values(['dimension', 'JS'], inplace=True, ascending=False)

    dimension_list = dta['dimension'].unique()
    # 计算EP
    dta['EP'] = ((dta['actual_1'] - dta['pred_1']) * dta['pred_sum_2'] - (dta['actual_2'] - dta['pred_2']) * dta[
        'pred_sum_1']) / (dta['pred_sum_2'] * (dta['pred_sum_2'] + dta['actual_2'] - dta['pred_2']))
    dta['EP_normalization'] = 0
    # 分维度EP归一化
    for d in dimension_list:
        EP_mms = dta[dta['dimension'] == d]['EP']
        EP_mms = EP_mms / EP_mms.sum()
        dta.loc[dta['dimension'] == d, 'EP_normalization'] = EP_mms

    Teep = 0.01
    Tep = 0.9

    # Adtributor
    ExplanatorySet = {}
    Candidate = {}
    for i in dta['dimension'].unique():
        indicator = []
        EP = 0
        for j in dta[dta['dimension'] == i]['indicator']:
            EP_ij = dta.loc[(dta['dimension'] == i) & (dta['indicator'] == j), 'EP_normalization'].values[0]
            if EP_ij > Teep:
                indicator.append(j)
                EP += EP_ij
            if EP > Tep:
                S = 0
                for k in indicator:
                    S_indicator = dta.loc[(dta['dimension'] == i) & (dta['indicator'] == k), 'JS'].values[0]
                    S += S_indicator
                    ExplanatorySet[str(i) + '-' + str(k)] = S_indicator
                Candidate[i] = S
                break
    Candidate_sorted_values = sorted(Candidate.items(), key=lambda x: x[1], reverse=True)
    ExplanatorySet_sorted_values = sorted(ExplanatorySet.items(), key=lambda x: x[1], reverse=True)
    return Candidate_sorted_values, ExplanatorySet_sorted_values, dta