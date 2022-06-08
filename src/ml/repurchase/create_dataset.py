#!/user/bin/env python
# -*- coding:utf-8 -*-

import datetime
from typing import Dict
from xmlrpc.client import Boolean
# from spark_env import SparkEnv
import digitforce.aip.common.utils.spark_helper as SparkEnv
import requests
import sys
import time
import json
from collections import Counter
import pandas as pd
from collections import defaultdict
import numpy as np
import os
import datetime
import time
from typing import List
from sklearn.preprocessing import LabelEncoder


class CreateDataset:
    def __init__(self):
        pass

    def ConstructFeatures(self,
                          train_period: str,
                          predict_period: str,
                          cat_list: str,
                          is_train,
                          order_table: str,
                          bh_table: str,
                          user_table: str,
                          item_table: str,
                          odinfo: Dict,
                          bhinfo: Dict,
                          usinfo: Dict,
                          itinfo: Dict,
                          bh_code_map: Dict,
                          where_sql: str
                          ):
        today = datetime.datetime.today()
        spark = SparkEnv.build_spark_session('Repurchase')
        # spark = None

        if is_train:
            cur_str = get_min_date_of_4_table(spark, user_table, order_table, item_table, bh_table, usinfo, odinfo,
                                              itinfo, bhinfo)
            #             print(cur_str)
            cur_str = (datetime.datetime.strptime(cur_str, '%Y-%m-%d') - datetime.timedelta(days=30)).strftime(
                '%Y-%m-%d')
            #             print(cur_str)
            feature_dates, train_date, predict_date = get_time_params(cur_str, train_period, predict_period)
            samples = get_samples_train(spark, order_table, odinfo, item_table, itinfo, cur_str, train_date, cat_list,
                                        predict_date)
        #             print(samples)
        else:
            # cur_str = today.strftime('%Y-%m-%d')
            cur_str = get_min_date_of_4_table(spark, user_table, order_table, item_table, bh_table, usinfo, odinfo,
                                              itinfo, bhinfo)
            feature_dates, train_date, predict_date = get_time_params(cur_str, train_period, predict_period)
            #             print(feature_dates, train_date, predict_date )
            samples = get_samples_predict(spark, order_table, odinfo, item_table, itinfo, cat_list, train_date, cur_str,
                                          where_sql, user_table, usinfo)
        #             print(samples)
        if len(samples) == 0:
            spark.stop()
            return samples
        else:
            features_of_order = get_order_features(spark, feature_dates, cur_str, order_table, odinfo, item_table,
                                                   itinfo, cat_list)

            features_of_user = get_label_features(spark, samples, user_table, usinfo, cur_str)

            features_of_behavior = get_behavior_features(spark, feature_dates, item_table, itinfo, bh_table, bhinfo,
                                                         bh_code_map, cur_str, cat_list)

            if not features_of_order.empty:
                data = pd.merge(samples, features_of_order, how='left', on='user_id')
            else:
                data = samples
            if not features_of_user.empty:
                data = pd.merge(data, features_of_user, how='left', on='user_id')
            if not features_of_behavior.empty:
                data = pd.merge(data, features_of_behavior, how='left', on='user_id')

            spark.stop()
            return data


def get_min_date_of_4_table(spark, user_table: str, order_table: str, item_table: str, bh_table: str, usinfo: Dict,
                            odinfo: Dict, itinfo: Dict, bhinfo: Dict):
    sql1 = '''select max({0}) as us_max_date from {1} '''.format(usinfo['dt'], user_table)
    sql2 = '''select substr(max({0}),1,10) as od_max_date from {1} '''.format(odinfo['order_time'], order_table)
    sql3 = '''select max({0}) as it_max_date from {1} '''.format(itinfo['dt'], item_table)
    sql4 = '''select substr(max({0}),1,10) as bh_max_date from {1} '''.format(bhinfo['event_time'], bh_table)
    dt1 = spark.sql(sql1).toPandas()
    dt2 = spark.sql(sql2).toPandas()
    dt3 = spark.sql(sql3).toPandas()
    dt4 = spark.sql(sql4).toPandas()
    #     print(dt1,dt2,dt3,dt4)
    min_date = min([dt1.iloc[0, 0], dt2.iloc[0, 0], dt3.iloc[0, 0], dt4.iloc[0, 0]])
    #     print(min_date)
    return min_date


def get_time_params(cur_str: str, train_period: str, predict_period: str):
    current_day = datetime.datetime.strptime(cur_str, "%Y-%m-%d")
    last_3_days = (current_day - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    last_7_days = (current_day - datetime.timedelta(days=6)).strftime("%Y-%m-%d")
    last_15_days = (current_day - datetime.timedelta(days=14)).strftime("%Y-%m-%d")
    last_1_month = (current_day - datetime.timedelta(days=29)).strftime("%Y-%m-%d")
    last_2_month = (current_day - datetime.timedelta(days=59)).strftime("%Y-%m-%d")
    next_day = (current_day + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    next_3_day = (current_day + datetime.timedelta(days=3)).strftime("%Y-%m-%d")
    next_7_day = (current_day + datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    next_15_day = (current_day + datetime.timedelta(days=15)).strftime("%Y-%m-%d")
    next_1_month = (current_day + datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    next_2_month = (current_day + datetime.timedelta(days=60)).strftime("%Y-%m-%d")
    if train_period == '过去15天':
        train_date = last_15_days
    elif train_period == '过去30天':
        train_date = last_1_month
    else:
        train_date = last_2_month
    if predict_period == '未来15天':
        predict_date = next_15_day
    elif predict_period == '未来30天':
        predict_date = next_1_month
    else:
        predict_date = next_2_month
    feature_dates = [last_2_month, last_1_month, last_15_days, last_7_days, last_3_days]
    return feature_dates, train_date, predict_date


def get_samples_train(spark, order_table, odinfo_map, item_table, itinfo_map, cur_str, train_date, cate_list,
                      predict_date):
    sql = '''
        select 
            a.user_id,
            if(b.user_id is null, 0, 1) as label
        from
        (
            select c.user_id as user_id from
            (select 
                {0} as user_id,
                {1} as sku
            from
                {2}
            where
                {3} between '{4}' and '{5}'
            group by {0},{1})as c
            left join
            (select 
                {6} as sku,
                {7} as cat
            from 
                {8}
            where {9} = '{5}'
                and {7} in {10})as d
            on c.sku=d.sku
            where d.sku is not null
            group by c.user_id
        ) as a
        left join
        (   
            select e.user_id as user_id from
            (select 
                {0} as user_id,
                {1} as sku
            from
                {2}
            where
                {3} > '{5}' and {3} <= '{11}'
            group by {0}, {1})as e
            left join
            (select 
                {6} as sku,
                {7} as cat
            from 
                {8}
            where {9} = '{11}'
                and {7} in {10})as f
            on e.sku=f.sku
            where f.sku is not null
            group by e.user_id
        )as b
        on a.user_id = b.user_id
    '''.format(odinfo_map['user_id'], odinfo_map['sku'], order_table, odinfo_map['order_time'], train_date, cur_str,
               itinfo_map['sku'], itinfo_map['cate'], item_table, itinfo_map['dt'], cate_list, predict_date)
    #     print(sql)
    samples = spark.sql(sql).toPandas()
    return samples


def get_samples_predict(spark, order_table, odinfo_map, item_table, itinfo_map, cate_list, train_date, cur_str,
                        where_sql, user_table, usinfo):
    sql = '''
        select
            a.user_id 
        from
        (select 
            {0} as user_id,
            {1} as sku
        from
            {2}
        where
            {3} between '{4}' and '{5}'
        group by {0},{1}) as a
        left join
        (select 
            {6} as sku
        from
            {7}
        where
            {8} = '{5}'
            and {9} in {10}) as b
        on a.sku=b.sku
        where b.sku is not null
        group by a.user_id    
    '''.format(odinfo_map['user_id'], odinfo_map['sku'], order_table, odinfo_map['order_time'], train_date, cur_str,
               itinfo_map['sku'], item_table, itinfo_map['dt'], itinfo_map['cate'], cate_list)
    #     print(sql)
    samples = spark.sql(sql).toPandas()
    if where_sql:
        sql_where = '''
            select {0} as user_id
            from {1}
            where {2} 
                and {3} = '{4}'
        '''.format(usinfo['user_id'], user_table, where_sql, usinfo['dt'], cur_str)
        samples1 = spark.sql(sql_where).toPandas()
        if not samples.empty and not samples1.empty:
            samples = pd.merge(samples1, samples, how='left', on='user_id')

    return samples


def get_order_features(spark, feature_dates: List[str], cur_str, order_table, odinfo_map, item_table, itinfo_map,
                       cate_list):
    last_x_days_name = ['2m', '1m', '15d', '7d', '3d']
    last_x_days = [60, 30, 15, 7, 3]
    orders_df = pd.DataFrame()
    sql_item = '''
        select
            {0} as sku
        from
            {1}
        where
            {2} = '{3}'
            and {4} in {5}
    '''.format(itinfo_map['sku'], item_table, itinfo_map['dt'], cur_str, itinfo_map['cate'], cate_list)
    #     print(sql_item)
    item_tmp = spark.sql(sql_item)
    item_tmp.createOrReplaceTempView('item_cat_tmp')

    for i in range(len(feature_dates)):
        last_x_day = feature_dates[i]
        #         print(last_x_days[i])
        # 计算近x天的订单量，购买商品种类数目，平均购买间隔（=（最大购买时间-最小购买时间）/购买天数）
        sql_orders_count = '''
            select
                {0} as user_id,
                count(distinct {1}) as {2},
                if(count(distinct {1})=1, {3}, round(datediff(from_unixtime(unix_timestamp(substr(max({4}),1,10),"yyyy-MM-dd"),"yyyy-MM-dd"),from_unixtime(unix_timestamp(substr(min({4}),1,10),"yyyy-MM-dd"),"yyyy-MM-dd"))/(count(distinct substr({4},1,10))-1),2)) as {5} --时间分区可能会变
            from {6}
            where {4} between '{7}' and '{8}'
                and {9} in (select sku from item_cat_tmp)
            group by {0}
        '''.format(odinfo_map['user_id'], odinfo_map['order_id'], 'od_cat_ct_' + last_x_days_name[i],
                   last_x_days[i] / 2, odinfo_map['order_time'], 'avg_cat_jg_' + last_x_days_name[i], order_table,
                   last_x_day, cur_str, odinfo_map['sku'])
        #         print(sql_orders_count)
        orders_count_df = spark.sql(sql_orders_count).toPandas()
        orders_count_df[['user_id']] = orders_count_df[['user_id']].astype(str)
        orders_count_df[['od_cat_ct_' + last_x_days_name[i], 'avg_cat_jg_' + last_x_days_name[i]]] = orders_count_df[
            ['od_cat_ct_' + last_x_days_name[i], 'avg_cat_jg_' + last_x_days_name[i]]].astype(np.float)

        if orders_df.empty:
            orders_df = orders_count_df
        else:
            orders_df = pd.merge(orders_df, orders_count_df, how='outer', on='user_id')
        #         print(orders_df)

        if odinfo_map['sale_quantity']:
            # 计算近x天的购买商品数量，平均购买商品数量
            sql_order_qty = '''
                select
                    {0} as user_id,
                    sum(coalesce({1}, 0)) as {2},
                    sum(coalesce({1},0)) / count(distinct {3}) as {4}
                from {5}
                where {6} between '{7}' and '{8}'
                    and {9} in (select sku from item_cat_tmp)
                group by {0}
            '''.format(odinfo_map['user_id'], odinfo_map['sale_quantity'], 'qty_cat_' + last_x_days_name[i],
                       odinfo_map['order_id'], 'qty_cat_avg_' + last_x_days_name[i], order_table,
                       odinfo_map['order_time'], last_x_day, cur_str, odinfo_map['sku'])
            #             print(sql_order_qty)
            order_qty_df = spark.sql(sql_order_qty).toPandas()
            order_qty_df[['user_id']] = order_qty_df[['user_id']].astype(str)
            order_qty_df[['qty_cat_' + last_x_days_name[i], 'qty_cat_avg_' + last_x_days_name[i]]] = order_qty_df[
                ['qty_cat_' + last_x_days_name[i], 'qty_cat_avg_' + last_x_days_name[i]]].astype(np.float)
            orders_df = pd.merge(orders_df, order_qty_df, how='outer', on='user_id')

        if odinfo_map['sale_amount']:
            # 计算近x天的购买金额，平均购买金额
            sql_order_amt = '''
                select
                    {0} as user_id,
                    sum(coalesce({1}, 0)) as {2},
                    sum(coalesce({1},0)) / count(distinct {3}) as {4}
                from {5}
                where {6} between '{7}' and '{8}'
                    and {9} in (select sku from item_cat_tmp)
                group by {0}
            '''.format(odinfo_map['user_id'], odinfo_map['sale_amount'], 'amt_cat_' + last_x_days_name[i],
                       odinfo_map['order_id'], 'amt_cat_avg_' + last_x_days_name[i], order_table,
                       odinfo_map['order_time'], last_x_day, cur_str, odinfo_map['sku'])
            #             print(sql_order_amt)
            order_amt_df = spark.sql(sql_order_amt).toPandas()
            order_amt_df[['user_id']] = order_amt_df[['user_id']].astype(str)
            order_amt_df[['amt_cat_' + last_x_days_name[i], 'amt_cat_avg_' + last_x_days_name[i]]] = order_amt_df[
                ['amt_cat_' + last_x_days_name[i], 'amt_cat_avg_' + last_x_days_name[i]]].astype(np.float)
            orders_df = pd.merge(orders_df, order_amt_df, how='outer', on='user_id')

        if i == 0:
            # 对近60天求最近末次购买距今天数
            sql_order_lastbuy = '''
                select
                    {0} as user_id,
                    if(count(distinct {1})=0, 60, round(datediff(from_unixtime(unix_timestamp("{2}","yyyy-MM-dd"),"yyyy-MM-dd"),from_unixtime(unix_timestamp(substr(max({1}),1,10),"yyyy-MM-dd"),"yyyy-MM-dd")),2)) as lastbuy_diff
                from {3}
                where {1} between '{4}' and '{2}'
                    and {5} in (select sku from item_cat_tmp)
                group by {0}
            '''.format(odinfo_map['user_id'], odinfo_map['order_time'], cur_str, order_table, last_x_day,
                       odinfo_map['sku'])
            #             print(sql_order_lastbuy)
            order_lastbuy_df = spark.sql(sql_order_lastbuy).toPandas()
            order_lastbuy_df[['user_id']] = order_lastbuy_df[['user_id']].astype(str)
            order_lastbuy_df[['lastbuy_diff']] = order_lastbuy_df[['lastbuy_diff']].astype(np.float)
            orders_df = pd.merge(orders_df, order_lastbuy_df, how='outer', on='user_id')

        # 以下特征为全品类：即不限制品类
        # 计算近x天的订单量，购买商品种类数目，平均购买间隔（=（最大购买时间-最小购买时间）/购买天数）
        sql_orders_count_all = '''
            select
                {0} as user_id,
                count(distinct {1}) as {2},
                if(count(distinct {1})=1, {3}, round(datediff(from_unixtime(unix_timestamp(substr(max({4}),1,10),"yyyy-MM-dd"),"yyyy-MM-dd"),from_unixtime(unix_timestamp(substr(min({4}),1,10),"yyyy-MM-dd"),"yyyy-MM-dd"))/(count(distinct substr({4},1,10))-1),2)) as {5} --时间分区可能会变
            from {6}
            where {4} between '{7}' and '{8}'
            group by {0}
        '''.format(odinfo_map['user_id'], odinfo_map['order_id'], 'od_ct_' + last_x_days_name[i], last_x_days[i] / 2,
                   odinfo_map['order_time'], 'avg_jg_' + last_x_days_name[i], order_table, last_x_day, cur_str)
        orders_count_all_df = spark.sql(sql_orders_count_all).toPandas()
        orders_count_all_df[['user_id']] = orders_count_all_df[['user_id']].astype(str)
        orders_count_all_df[['od_ct_' + last_x_days_name[i], 'avg_jg_' + last_x_days_name[i]]] = orders_count_all_df[
            ['od_ct_' + last_x_days_name[i], 'avg_jg_' + last_x_days_name[i]]].astype(np.float)
        if orders_df.empty:
            orders_df = orders_count_all_df
        else:
            orders_df = pd.merge(orders_df, orders_count_all_df, how='outer', on='user_id')

        if odinfo_map['sale_quantity']:
            # 计算近x天的购买商品数量，平均购买商品数量
            sql_order_qty_all = '''
                select
                    {0} as user_id,
                    sum(coalesce({1}, 0)) as {2},
                    sum(coalesce({1},0)) / count(distinct {3}) as {4}
                from {5}
                where {6} between '{7}' and '{8}'
                group by {0}
            '''.format(odinfo_map['user_id'], odinfo_map['sale_quantity'], 'qty_' + last_x_days_name[i],
                       odinfo_map['order_id'], 'qty_avg_' + last_x_days_name[i], order_table, odinfo_map['order_time'],
                       last_x_day, cur_str)
            order_qty_all_df = spark.sql(sql_order_qty_all).toPandas()
            order_qty_all_df[['user_id']] = order_qty_all_df[['user_id']].astype(str)
            order_qty_all_df[['qty_' + last_x_days_name[i], 'qty_avg_' + last_x_days_name[i]]] = order_qty_all_df[
                ['qty_' + last_x_days_name[i], 'qty_avg_' + last_x_days_name[i]]].astype(np.float)
            orders_df = pd.merge(orders_df, order_qty_all_df, how='outer', on='user_id')

        if odinfo_map['sale_amount']:
            # 计算近x天的购买金额，平均购买金额
            sql_order_amt_all = '''
                select
                    {0} as user_id,
                    sum(coalesce({1}, 0)) as {2},
                    sum(coalesce({1},0)) / count(distinct {3}) as {4}
                from {5}
                where {6} between '{7}' and '{8}'
                group by {0}
            '''.format(odinfo_map['user_id'], odinfo_map['sale_amount'], 'amt_' + last_x_days_name[i],
                       odinfo_map['order_id'], 'amt_avg_' + last_x_days_name[i], order_table, odinfo_map['order_time'],
                       last_x_day, cur_str)
            order_amt_all_df = spark.sql(sql_order_amt_all).toPandas()
            order_amt_all_df[['user_id']] = order_amt_all_df[['user_id']].astype(str)
            order_amt_all_df[['amt_' + last_x_days_name[i], 'amt_avg_' + last_x_days_name[i]]] = order_amt_all_df[
                ['amt_' + last_x_days_name[i], 'amt_avg_' + last_x_days_name[i]]].astype(np.float)
            orders_df = pd.merge(orders_df, order_amt_all_df, how='outer', on='user_id')
    #     print(orders_df)
    return orders_df


def get_behavior_features(spark, feature_dates: List[str], item_table, itinfo_map, behavior_table, bhinfo_map,
                          bh_code_map, cur_str, cate_list):
    last_x_days_name = ['2m', '1m', '15d', '7d', '3d']
    bh_df = pd.DataFrame()
    sql_item = '''
        select
            {0} as sku
        from
            {1}
        where
            {2} = '{3}'
            and {4} in {5}
    '''.format(itinfo_map['sku'], item_table, itinfo_map['dt'], cur_str, itinfo_map['cate'], cate_list)
    #     print(sql_item)
    item_tmp = spark.sql(sql_item)
    item_tmp.createOrReplaceTempView('item_cat_tmp')

    for i in range(len(feature_dates)):
        last_x_day = feature_dates[i]
        if bh_code_map['click']:
            sql_dianji_cat = '''
                select
                    {0} as user_id,
                    count({1}) as {2}
                from
                    {3}
                where
                    {4} between '{5}' and '{6}' -- Time
                    and {1}='{7}' -- event_code
                    and {8} in (select sku from item_cat_tmp) -- sku
                group by {0}
            '''.format(bhinfo_map['user_id'], bhinfo_map['event_code'], 'dianji_cat_' + last_x_days_name[i],
                       behavior_table, bhinfo_map['event_time'], last_x_day, cur_str, bh_code_map['click'],
                       bhinfo_map['sku'])

            sql_dianji_all = '''
                select
                    {0} as user_id,
                    count({1}) as {2}
                from
                    {3}
                where
                    {4} between '{5}' and '{6}' -- Time
                    and {1}='{7}' -- event_code
                group by {0}
            '''.format(bhinfo_map['user_id'], bhinfo_map['event_code'], 'dianji_all_' + last_x_days_name[i],
                       behavior_table, bhinfo_map['event_time'], last_x_day, cur_str, bh_code_map['click'])
            #             print(sql_dianji_cat)
            #             print(sql_dianji_all)
            dianji_cat_df = spark.sql(sql_dianji_cat).toPandas()
            dianji_cat_df[['user_id']] = dianji_cat_df[['user_id']].astype(str)
            dianji_cat_df[['dianji_cat_' + last_x_day]] = dianji_cat_df[['dianji_cat_' + last_x_days_name[i]]].astype(
                np.int)
            dianji_all_df = spark.sql(sql_dianji_all).toPandas()
            dianji_all_df[['user_id']] = dianji_all_df[['user_id']].astype(str)
            dianji_all_df['dianji_all_' + last_x_day] = dianji_all_df[['dianji_all_' + last_x_days_name[i]]].astype(
                np.int)

            if bh_df.empty:
                bh_df = dianji_cat_df
            else:
                bh_df = pd.merge(bh_df, dianji_cat_df, how='outer', on='user_id')
                bh_df = pd.merge(bh_df, dianji_all_df, how='outer', on='user_id')

        if bh_code_map['cart_add']:
            sql_jiagou_cat = '''
                select
                    {0} as user_id,
                    count({1}) as {2}
                from
                    {3}
                where
                    {4} between '{5}' and '{6}' -- Time
                    and {1}='{7}' -- event_code
                    and {8} in (select sku from item_cat_tmp) -- sku
                group by {0}
            '''.format(bhinfo_map['user_id'], bhinfo_map['event_code'], 'jiagou_cat_' + last_x_days_name[i],
                       behavior_table, bhinfo_map['event_time'], last_x_day, cur_str, bh_code_map['cart_add'],
                       bhinfo_map['sku'])
            sql_jiagou_all = '''
                select
                    {0} as user_id,
                    count({1}) as {2}
                from
                    {3}
                where
                    {4} between '{5}' and '{6}' -- Time
                    and {1}='{7}' -- event_code
               group by {0}
            '''.format(bhinfo_map['user_id'], bhinfo_map['event_code'], 'jiagou_all_' + last_x_days_name[i],
                       behavior_table, bhinfo_map['event_time'], last_x_day, cur_str, bh_code_map['cart_add'])
            #             print(sql_jiagou_cat)
            #             print(sql_jiagou_all)
            jiagou_cat_df = spark.sql(sql_jiagou_cat).toPandas()
            jiagou_cat_df[['user_id']] = jiagou_cat_df[['user_id']].astype(str)
            jiagou_cat_df[['jiagou_cat_' + last_x_day]] = jiagou_cat_df[['jiagou_cat_' + last_x_days_name[i]]].astype(
                np.int)
            jiagou_all_df = spark.sql(sql_jiagou_all).toPandas()
            jiagou_all_df[['user_id']] = jiagou_all_df[['user_id']].astype(str)
            jiagou_all_df['jiagou_all_' + last_x_day] = jiagou_all_df[['jiagou_all_' + last_x_days_name[i]]].astype(
                np.int)

            if bh_df.empty:
                bh_df = jiagou_cat_df
            else:
                bh_df = pd.merge(bh_df, jiagou_cat_df, how='outer', on='user_id')
                bh_df = pd.merge(bh_df, jiagou_all_df, how='outer', on='user_id')

        if bh_code_map['browse']:
            sql_view_cat = '''
                select
                    {0} as user_id,
                    count({1}) as {2}
                from
                    {3}
                where
                    {4} between '{5}' and '{6}' -- Time
                    and {1}='{7}' -- event_code
                    and {8} in (select sku from item_cat_tmp) -- sku
                group by {0}
            '''.format(bhinfo_map['user_id'], bhinfo_map['event_code'], 'view_cat_' + last_x_days_name[i],
                       behavior_table, bhinfo_map['event_time'], last_x_day, cur_str, bh_code_map['browse'],
                       bhinfo_map['sku'])
            sql_view_all = '''
                select
                    {0} as user_id,
                    count({1}) as {2}
                from
                    {3}
                where
                    {4} between '{5}' and '{6}' -- Time
                    and {1}='{7}' -- event_code
                group by {0}    
            '''.format(bhinfo_map['user_id'], bhinfo_map['event_code'], 'view_all_' + last_x_days_name[i],
                       behavior_table, bhinfo_map['event_time'], last_x_day, cur_str, bh_code_map['browse'])
            view_cat_df = spark.sql(sql_view_cat).toPandas()
            view_cat_df[['user_id']] = view_cat_df[['user_id']].astype(str)
            view_cat_df[['view_cat_' + last_x_day]] = view_cat_df[['view_cat_' + last_x_days_name[i]]].astype(np.int)
            view_all_df = spark.sql(sql_view_all).toPandas()
            view_all_df[['user_id']] = view_all_df[['user_id']].astype(str)
            view_all_df['view_all_' + last_x_day] = view_all_df[['view_all_' + last_x_days_name[i]]].astype(np.int)

            if bh_df.empty:
                bh_df = view_cat_df
            else:
                bh_df = pd.merge(bh_df, view_cat_df, how='outer', on='user_id')
                bh_df = pd.merge(bh_df, view_all_df, how='outer', on='user_id')
    #     print(bh_df)
    return bh_df


def get_label_features(spark, samples: pd.DataFrame, user_table, usinfo_map, cur_str):
    sample_spark_df = spark.createDataFrame(samples)
    sample_spark_df.createOrReplaceTempView('samples_tmp')
    userlabel_df = pd.DataFrame()
    le = LabelEncoder()
    # 待改进：可合并
    if ('sex' in usinfo_map) and usinfo_map['sex'] != '':
        sql_gender = '''
             select 
                a.user_id,
                b.gender
            from
                (select distinct user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as gender 
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id

        '''.format(usinfo_map['user_id'], usinfo_map['sex'], user_table, usinfo_map['dt'], cur_str)
        #         print(sql_gender)
        gender_df = spark.sql(sql_gender).toPandas()
        gender_df.fillna(value=0, inplace=True)
        gender_df[['user_id', 'gender']] = gender_df[['user_id', 'gender']].astype(str)
        if userlabel_df.empty:
            userlabel_df = gender_df
        else:
            userlabel_df = pd.merge(userlabel_df, gender_df, how='inner', on='user_id')
        userlabel_df['gender'] = le.fit_transform(userlabel_df['gender'])

    if ('age' in usinfo_map) and usinfo_map['age'] != '':
        sql_age = '''
            select 
                a.user_id,
                b.age
            from
                (select user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as age 
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id

        '''.format(usinfo_map['user_id'], usinfo_map['age'], user_table, usinfo_map['dt'], cur_str)
        age_df = spark.sql(sql_age).toPandas()
        age_df.fillna(value=0, inplace=True)
        age_df[['user_id']] = age_df[['user_id']].astype(str)
        age_df[['age']] = age_df[['age']].astype(np.int64)
        if userlabel_df.empty:
            userlabel_df = age_df
        else:
            userlabel_df = pd.merge(userlabel_df, age_df, how='inner', on='user_id')

    if ('city' in usinfo_map) and usinfo_map['city'] != '':
        sql_city = '''
            select 
                a.user_id,
                b.city
            from
                (select user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as city 
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id

        '''.format(usinfo_map['user_id'], usinfo_map['city'], user_table, usinfo_map['dt'], cur_str)
        city_df = spark.sql(sql_city).toPandas()
        city_df.fillna(value=0, inplace=True)
        city_df[['user_id', 'city']] = city_df[['user_id', 'city']].astype(str)
        if userlabel_df.empty:
            userlabel_df = city_df
        else:
            userlabel_df = pd.merge(userlabel_df, city_df, how='inner', on='user_id')
        userlabel_df['city'] = le.fit_transform(userlabel_df['city'])

    if ('consume_levle' in usinfo_map) and usinfo_map['consume_level'] != '':
        sql_consume_lvl = '''
            select 
                a.user_id,
                b.consume_lvl
            from
                (select user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as consume_lvl 
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id

        '''.format(usinfo_map['user_id'], usinfo_map['consume_level'], user_table, usinfo_map['dt'], cur_str)
        consume_lvl_df = spark.sql(sql_consume_lvl).toPandas()
        consume_lvl_df.fillna(value=0, inplace=True)
        consume_lvl_df[['user_id', 'consume_lvl']] = consume_lvl_df[['user_id', 'consume_lvl']].astype(str)
        if userlabel_df.empty:
            userlabel_df = consume_lvl_df
        else:
            userlabel_df = pd.merge(userlabel_df, consume_lvl_df, how='inner', on='user_id')
        userlabel_df['consume_lvl'] = le.fit_transform(userlabel_df['consume_lvl'])

    if ('online_signup_time' in usinfo_map) and usinfo_map['online_signup_time'] != '':
        sql_sign_on = '''
            select 
                a.user_id,
                b.sign_on_days
            from
                (select user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    if({1} is null, 0, round(datediff(from_unixtime(unix_timestamp("{2}","yyyy-MM-dd"),"yyyy-MM-dd"),from_unixtime(unix_timestamp(substr({1},1,10),"yyyy-MM-dd"),"yyyy-MM-dd")),0)) as sign_on_days 
                from {4}
                where {3} = '{2}' ) as b
            on a.user_id = b.user_id

        '''.format(usinfo_map['user_id'], usinfo_map['online_signup_time'], cur_str, usinfo_map['dt'], user_table)
        #         print(sql_sign_on)
        sign_on_df = spark.sql(sql_sign_on).toPandas()
        sign_on_df.fillna(value=0, inplace=True)
        sign_on_df[['user_id']] = sign_on_df[['user_id']].astype(str)
        sign_on_df[['sign_on_days']] = sign_on_df[['sign_on_days']].astype(np.int64)
        if userlabel_df.empty:
            userlabel_df = sign_on_df
        else:
            userlabel_df = pd.merge(userlabel_df, sign_on_df, how='inner', on='user_id')

    if ('recent_view_day' in usinfo_map) and usinfo_map['recent_view_day'] != '':
        sql_latest_view = '''
            select 
                a.user_id,
                b.latest_view_days
            from
                (select user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    if({1} is null, 0, round(datediff(from_unixtime(unix_timestamp("{2}","yyyy-MM-dd"),"yyyy-MM-dd"),from_unixtime(unix_timestamp(substr({1},1,10),"yyyy-MM-dd"),"yyyy-MM-dd")),0)) as latest_view_days
                from {4}
                where {3} = '{2}' 
               ) as b
            on a.user_id = b.user_id

        '''.format(usinfo_map['user_id'], usinfo_map['recent_view_day'], cur_str, usinfo_map['dt'], user_table)
        latest_view_df = spark.sql(sql_latest_view).toPandas()
        latest_view_df.fillna(value=0, inplace=True)
        latest_view_df[['user_id']] = latest_view_df[['user_id']].astype(str)
        latest_view_df[['latest_view_days']] = latest_view_df[['latest_view_days']].astype(np.int64)
        if userlabel_df.empty:
            userlabel_df = latest_view_df
        else:
            userlabel_df = pd.merge(userlabel_df, latest_view_df, how='inner', on='user_id')

    # if  usinfo_map['是否线上新客']:
    #     sql_consume_online = '''
    #         select
    #             a.user_id,
    #             b.is_consume_online
    #         from
    #             (select user_id from samples_tmp) as a
    #         left join
    #             (select
    #                 {0} as user_id,
    #                 {1} as is_consume_online
    #             from {2}
    #             where {3} = {4} ) as b
    #         on a.user_id = b.user_id
    #         where b.user_id is not null
    #     '''.format( usinfo_map['user_id'],  usinfo_map['是否线上新客'],  user_table,  usinfo_map['dt'],  cur_str)
    #     consume_online_df =  spark.sql(sql_consume_online).toPandas()
    #     consume_online_df[['user_id','is_consume_online']] =  consume_online_df[['user_id','is_consume_online']].astype(str)
    #     if not userlabel_df:
    #         userlabel_df = consume_online_df
    #     else:
    #         userlabel_df = pd.merge(userlabel_df, consume_online_df, how='inner', on='user_id')

    if ('life_stage' in usinfo_map) and usinfo_map['life_stage'] != '':
        sql_consume_online = '''
            select 
                a.user_id,
                b.life_stage
            from
                (select user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as life_stage
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id

        '''.format(usinfo_map['user_id'], usinfo_map['life_stage'], user_table, usinfo_map['dt'], cur_str)
        consume_online_df = spark.sql(sql_consume_online).toPandas()
        consume_online_df.fillna(value=0, inplace=True)
        consume_online_df[['user_id', 'life_stage']] = consume_online_df[['user_id', 'life_stage']].astype(str)
        if userlabel_df.empty:
            userlabel_df = consume_online_df
        else:
            userlabel_df = pd.merge(userlabel_df, consume_online_df, how='inner', on='user_id')
        userlabel_df['life_stage'] = le.fit_transform(userlabel_df['life_stage'])

    if ('is_new' in usinfo_map) and usinfo_map['is_new'] != '':
        sql_consume_online = '''
            select 
                a.user_id,
                b.is_new
            from
                (select user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as is_new
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id

        '''.format(usinfo_map['user_id'], usinfo_map['is_new'], user_table, usinfo_map['dt'], cur_str)
        consume_online_df = spark.sql(sql_consume_online).toPandas()
        consume_online_df.fillna(value=0, inplace=True)
        consume_online_df[['user_id', 'is_new']] = consume_online_df[['user_id', 'is_new']].astype(str)
        if userlabel_df.empty:
            userlabel_df = consume_online_df
        else:
            userlabel_df = pd.merge(userlabel_df, consume_online_df, how='inner', on='user_id')
        userlabel_df['is_new'] = le.fit_transform(userlabel_df['is_new'])

    if ('is_consume_online' in usinfo_map) and usinfo_map['is_consume_online'] != '':
        sql_consume_online = '''
            select 
                a.user_id,
                b.is_consume_online
            from
                (select user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as is_consume_online
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id

        '''.format(usinfo_map['user_id'], usinfo_map['is_consume_online'], user_table, usinfo_map['dt'], cur_str)
        consume_online_df = spark.sql(sql_consume_online).toPandas()
        consume_online_df.fillna(value=0, inplace=True)
        consume_online_df[['user_id', 'is_consume_online']] = consume_online_df[
            ['user_id', 'is_consume_online']].astype(str)
        if userlabel_df.empty:
            userlabel_df = consume_online_df
        else:
            userlabel_df = pd.merge(userlabel_df, consume_online_df, how='inner', on='user_id')
        userlabel_df['is_consume_online'] = le.fit_transform(userlabel_df['is_consume_online'])
    #     print(userlabel_df)
    return userlabel_df





