import datetime
import math
import traceback
from typing import Dict
from spark_env import SparkEnv, spark_read
import requests
import sys
import time
import json
from collections import Counter
import pandas as pd
from collections import defaultdict
import pyhdfs
import numpy as np
import os
import datetime
import time
from typing import List
import digitforce.aip.common.utils.config_helper as config_helper
from preprocessing.utils import Negative_Sample
import logging
hdfs_config = config_helper.get_module_config("hdfs")


class CreateDataset:
    def __init__(self):
        pass

    def ConstructFeatures(self, is_train, userData, bhData, orderData, goodsData, eventCode):
        """
        Function: get samples and features
        :Return type : DataFrame
        """
        spark = SparkEnv('lookalike_train').spark
        print("启动spark")
        if is_train:
            spark_read(spark, userData['tableName'], 'user_table_tmp_lookalike', 'dt', "2022-06-24", "2022-06-24")
            userData['tableName'] = 'user_table_tmp_lookalike'
            spark_read(spark, bhData['tableName'], 'bh_table_tmp_lookalike', 'dt', "2022-05-23", "2022-06-23")
            bhData['tableName'] = 'bh_table_tmp_lookalike'
            spark_read(spark, orderData['tableName'], 'order_table_tmp_lookalike', 'dt', "2022-05-23", "2022-06-23")
            orderData['tableName'] = 'order_table_tmp_lookalike'
            spark_read(spark, goodsData['tableName'], 'item_table_tmp_lookalike', 'dt', "2022-09-05", "2022-09-05")
            goodsData['tableName'] = 'item_table_tmp_lookalike'

            cur_str = get_cur_date(spark, userData, bhData, orderData, goodsData)

            print("构建特征所使用数据日期为：{}".format(cur_str))
            #         cur_str = '2022-03-18'
            current_day = datetime.datetime.strptime(cur_str, "%Y-%m-%d")
            # last_3_day = (current_day - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
            # last_7_day = (current_day - datetime.timedelta(days=6)).strftime("%Y-%m-%d")
            # last_14_day = (current_day - datetime.timedelta(days=13)).strftime("%Y-%m-%d")
            last_1_month = (current_day - datetime.timedelta(days=29)).strftime("%Y-%m-%d")

            user_dates_features = [last_1_month]
            item_dates_features = [last_1_month]
            samples = get_samples_train(spark, bhData, last_1_month, cur_str, eventCode)
            print("构建模型训练样本共计{}条".format(len(samples)))
            user_features_of_order, item_features_of_order = get_order_features(spark, is_train, samples,
                                                                                user_dates_features,
                                                                                item_dates_features,
                                                                                cur_str, orderData, goodsData)
            user_features_of_bh, item_features_of_bh = get_bh_features(spark, is_train, samples,
                                                                       user_dates_features,
                                                                       item_dates_features,
                                                                       cur_str, bhData, goodsData, eventCode)
            features_of_user = get_user_features(spark, samples, userData, "2022-06-24")
            features_of_item = get_item_features(spark, samples, goodsData, cur_str)
            # 特征聚合
            samples = pd.merge(samples, user_features_of_order, how='left', on='user_id')
            samples = pd.merge(samples, item_features_of_order, how='left', on='sku')
            samples = pd.merge(samples, user_features_of_bh, how='left', on='user_id')
            samples = pd.merge(samples, item_features_of_bh, how='left', on='sku')
            samples = pd.merge(samples, features_of_user, how='left', on='user_id')
            samples = pd.merge(samples, features_of_item, how='left', on='sku')
        else:
            cur_str = get_cur_date(spark, userData, bhData, orderData, goodsData)

            print("构建特征所使用数据日期为：{}".format(cur_str))
            #         cur_str = '2022-03-18'
            current_day = datetime.datetime.strptime(cur_str, "%Y-%m-%d")
            # last_3_day = (current_day - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
            # last_7_day = (current_day - datetime.timedelta(days=6)).strftime("%Y-%m-%d")
            # last_14_day = (current_day - datetime.timedelta(days=13)).strftime("%Y-%m-%d")
            last_1_month = (current_day - datetime.timedelta(days=29)).strftime("%Y-%m-%d")

            user_dates_features = [last_1_month]
            item_dates_features = [last_1_month]

            samples = get_user_list(spark, userData, "2022-06-24")
            print("构建全量用户样本共计{}条".format(len(samples)))
            user_features_of_order, item_features_of_order = get_order_features(spark, is_train, samples,
                                                                                user_dates_features,
                                                                                item_dates_features,
                                                                                cur_str, orderData, goodsData)
            user_features_of_bh, item_features_of_bh = get_bh_features(spark, is_train, samples,
                                                                       user_dates_features,
                                                                       item_dates_features,
                                                                       cur_str, bhData, goodsData, eventCode)
            features_of_user = get_user_features(spark, samples, userData, "2022-06-24")
            # 特征聚合
            samples = pd.merge(samples, user_features_of_order, how='left', on='user_id')
            samples = pd.merge(samples, user_features_of_bh, how='left', on='user_id')
            samples = pd.merge(samples, features_of_user, how='left', on='user_id')
        return samples


    def Filter_and_Construct(self, orderData, bhData, userData, goodsData, scene, expansion_dimension,
                             filter_condition):
        spark = SparkEnv('lookalike_crowd').spark
        spark_read(spark, userData['tableName'], 'user_table_tmp_lookalike', 'dt', "2022-06-24", "2022-06-24")
        userData['tableName'] = 'user_table_tmp_lookalike'
        spark_read(spark, bhData['tableName'], 'bh_table_tmp_lookalike', 'dt', "2022-05-23", "2022-06-23")
        bhData['tableName'] = 'bh_table_tmp_lookalike'
        spark_read(spark, orderData['tableName'], 'order_table_tmp_lookalike', 'dt', "2022-05-23", "2022-06-23")
        orderData['tableName'] = 'order_table_tmp_lookalike'
        spark_read(spark, goodsData['tableName'], 'item_table_tmp_lookalike', 'dt', "2022-06-23", "2022-06-23")
        goodsData['tableName'] = 'item_table_tmp_lookalike'

        cur_str = get_cur_date(spark, userData, bhData, orderData, goodsData)
        print("构建特征所使用数据日期为：" + cur_str)
        current_day = datetime.datetime.strptime(cur_str, "%Y-%m-%d")
        #         last_3_day = (current_day - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
        #         last_7_day = (current_day - datetime.timedelta(days=6)).strftime("%Y-%m-%d")
        #         last_14_day = (current_day - datetime.timedelta(days=13)).strftime("%Y-%m-%d")
        last_1_month = (current_day - datetime.timedelta(days=29)).strftime("%Y-%m-%d")

        user_dates_features = [last_1_month]
        item_dates_features = [last_1_month]
        #         spark = SparkEnv('test_lookalike_crowd')
        if filter_condition:
            samples = crowd_filter(spark, filter_condition, userData, "2022-06-24")
        else:
            samples = get_user_list(spark, userData, "2022-06-24")
        if len(samples) != 0 and scene != "1":
            samples = crowd_select_scence(spark, samples, orderData, goodsData, cur_str, scene, expansion_dimension)

        return samples


def get_cur_date(spark, userData, bhData, orderData, goodsData):
    date_user = get_lastest_date(spark, userData, userData['dt'])
    date_bh = get_lastest_date(spark, bhData, bhData['event_time'])
    date_order = get_lastest_date(spark, orderData, orderData['order_time'])
    date_goods = get_lastest_date(spark, goodsData, goodsData['dt'])
    date_df = pd.concat([date_user, date_bh, date_order, date_goods], ignore_index=True)
    cur_date = sorted(date_df['dt'])[0]
    return str(cur_date)


def get_lastest_date(spark, data, date_column):
    sql_date = '''
        select
            max(DATE_FORMAT({0}, 'yyyy-MM-dd')) as dt
        from
            {1}
    '''.format(date_column, data['tableName'])
    lastest_date_df = spark.sql(sql_date).toPandas()
    return lastest_date_df


def get_samples_train(spark, bhData, start_day, end_day, eventCode):
    '''
    从流量表抽取数据
    :param spark:
    :param bhData:
    :param start_day:
    :param end_day:
    :param eventCode:
    :return:
    '''
    # try:
    sql_pos = '''
            select 
                {0} as user_id,
                {1} as sku,
                1 as tag
            from
                {2}
            where
                {3} > '{4}' 
            and {3} <= '{5}'
            and {6} = '{7}'
            group by {0}, {1}
            distribute by rand() sort by rand() limit 200000
    '''.format(bhData['user_id'], bhData['sku'], bhData['tableName'], bhData['event_time'],
               start_day, end_day, bhData['event_code'], eventCode['click'])

    sql_neg = '''
            select 
                {0} as user_id,
                {1} as sku,
                0 as tag
            from
                {2}
            where
                {3} > '{4}' 
            and {3} <= '{5}'
            and {6} = '{7}'
            group by {0}, {1}
            distribute by rand() sort by rand() limit 200000
    '''.format(bhData['user_id'], bhData['sku'], bhData['tableName'], bhData['event_time'],
               start_day, end_day, bhData['event_code'], eventCode['exposure'])
    samples_pos = spark.sql(sql_pos).toPandas()
    samples_neg = spark.sql(sql_neg).toPandas()
    samples = pd.concat([samples_pos, samples_neg])
    samples = Negative_Sample(samples, 'user_id', 'sku', 'tag', ratio=2, method_id=0)
    samples[['user_id', 'sku']] = samples[['user_id', 'sku']].astype(str)
    #     print("sql_pos:" + sql_pos)
    #     print("sql_neg:" + sql_neg)
    #     samples = "test"
    return samples
    # except:
    #     print('Raise errors when constructing samples in training process.')


def get_user_list(spark, userData, cur_str):
    '''
    从用户表获取全量用户
    :param spark: spark连接对象
    :param userData['tableName: 用户表名称
    :param userData: 用户表字段映射
    :param cur_day: 当前日期
    :return: 用户列表
    '''
    sql_user_list = '''
        select
            distinct {0} as user_id
        from
            {1}
        where 
            {2} = '{3}'
    '''.format(userData['user_id'], userData['tableName'], userData['dt'], cur_str)
    user_list = spark.sql(sql_user_list).toPandas()
    #     print('get_user_list_sql' + sql_user_list)
    #     user_list = 'test'
    return user_list


def get_order_features(spark, is_train, samples, user_dates_features, item_dates_features, end_day, orderData,
                       goodsData):
    last_x_days_name = ['1m']
    last_x_days = [30]
    sample_spark_df = spark.createDataFrame(samples)
    sample_spark_df.createOrReplaceTempView('samples_tmp')
    user_features_of_order = None
    item_features_of_order = None

    for i in range(len(user_dates_features)):
        last_x_day = user_dates_features[i]

        # 以下特征为全品类：即不限制品类
        # 计算近x天的订单量，平均购买间隔（=（最大购买时间-最小购买时间）/购买天数）
        sql_user_orders_count = '''
            select
                b.{0} as user_id,
                count(distinct b.{1}) as {2},
                datediff('{8}',min(to_date(b.{4}))) as user_last_buy_diff,
                if(count(distinct b.{1})=1, {3}, datediff(max(to_date(b.{4})),min(to_date(b.{4}))))/(count(distinct to_date(b.{4}))-1) as {5} --时间分区可能会变
            from (select distinct user_id from samples_tmp) as a
            left join {6} as b
            on a.user_id = b.{0}
            where b.{4} between '{7}' and '{8}'
            group by b.{0}
        '''.format(orderData['user_id'], orderData['order_id'], 'od_ct_' + last_x_days_name[i], last_x_days[i] / 2,
                   orderData['order_time'], 'avg_jg_' + last_x_days_name[i], orderData['tableName'], last_x_day,
                   end_day)
        #         print('sql_user_orders_count' + sql_user_orders_count)
        user_orders_count_df = spark.sql(sql_user_orders_count).toPandas()
        user_orders_count_df[['user_id']] = user_orders_count_df[['user_id']].astype(str)
        user_orders_count_df[['od_ct_' + last_x_days_name[i], 'avg_jg_' + last_x_days_name[i]]] = user_orders_count_df[
            ['od_ct_' + last_x_days_name[i], 'avg_jg_' + last_x_days_name[i]]].astype(np.float)
        if not user_features_of_order:
            user_features_of_order = user_orders_count_df
        else:
            user_features_of_order = pd.merge(user_features_of_order, user_orders_count_df, how='left', on='user_id')

        if orderData['sale_quantity']:
            # 计算近x天的购买商品数量，平均购买商品数量
            sql_user_order_qty = '''
                select
                    b.{0} as user_id,
                    sum(coalesce(b.{1}, 0)) as {2},
                    sum(coalesce(b.{1},0)) / count(distinct b.{3}) as {4}
                from (select distinct user_id from samples_tmp) as a
                left join {5} as b
                on a.user_id = b.{0}
                where b.{6} between '{7}' and '{8}'
                group by b.{0}
            '''.format(orderData['user_id'], orderData['sale_quantity'], 'qty_' + last_x_days_name[i],
                       orderData['order_id'],
                       'qty_avg_' + last_x_days_name[i], orderData['tableName'], orderData['order_time'], last_x_day,
                       end_day)
            #             print('sql_user_order_qty' + sql_user_order_qty)
            user_order_qty_df = spark.sql(sql_user_order_qty).toPandas()
            user_order_qty_df[['user_id']] = user_order_qty_df[['user_id']].astype(str)
            user_order_qty_df[['qty_' + last_x_days_name[i], 'qty_avg_' + last_x_days_name[i]]] = user_order_qty_df[
                ['qty_' + last_x_days_name[i], 'qty_avg_' + last_x_days_name[i]]].astype(np.float)
            user_features_of_order = pd.merge(user_features_of_order, user_order_qty_df, how='left', on='user_id')

        if orderData['sale_amount']:
            # 计算近x天的购买金额，平均购买金额
            sql_user_order_amt = '''
                select
                    b.{0} as user_id,
                    sum(coalesce(b.{1}, 0)) as {2},
                    sum(coalesce(b.{1},0)) / count(distinct b.{3}) as {4}
                from (select distinct user_id from samples_tmp) as a
                left join {5} as b
                on a.user_id = b.{0}
                where b.{6} between '{7}' and '{8}'
                group by b.{0}
            '''.format(orderData['user_id'], orderData['sale_amount'], 'amt_' + last_x_days_name[i],
                       orderData['order_id'],
                       'amt_avg_' + last_x_days_name[i], orderData['tableName'], orderData['order_time'], last_x_day,
                       end_day)
            #             print('sql_user_order_amt' + sql_user_order_amt)
            user_order_amt_df = spark.sql(sql_user_order_amt).toPandas()
            user_order_amt_df[['user_id']] = user_order_amt_df[['user_id']].astype(str)
            user_order_amt_df[['amt_' + last_x_days_name[i], 'amt_avg_' + last_x_days_name[i]]] = user_order_amt_df[
                ['amt_' + last_x_days_name[i], 'amt_avg_' + last_x_days_name[i]]].astype(np.float)
            user_features_of_order = pd.merge(user_features_of_order, user_order_amt_df, how='left', on='user_id')

        if goodsData['cate']:
            sql_user_cate_count = '''
                        select
                            b.{0} as user_id,
                            count(distinct c.cate) as {1}
                        from (select distinct user_id from samples_tmp) as a
                        left join {2} as b
                        on a.user_id = b.{0}
                        left join
                        (
                            select {3} as sku,
                                   {4} as cate
                            from {5}
                            where 
                                {10} = '{7}'
                        ) as c
                        on b.{8} = c.sku
                        where b.{6} between '{9}' and '{7}'
                        group by b.{0}
                    '''.format(orderData['user_id'], 'cate_ct_' + last_x_days_name[i], orderData['tableName'],
                               goodsData['sku'], goodsData['cate'],
                               goodsData['tableName'], orderData['order_time'], end_day, orderData['sku'], last_x_day,
                               goodsData['dt'])
            #             print('sql_user_cate_count' + sql_user_cate_count)
            user_cate_count_df = spark.sql(sql_user_cate_count).toPandas()
            user_cate_count_df[['user_id']] = user_cate_count_df[['user_id']].astype(str)
            user_features_of_order = pd.merge(user_features_of_order, user_cate_count_df, how='left', on='user_id')

    if is_train:
        # 商品提取特征写死30天，需要的话再调整item_dates_features即可
        start_day = item_dates_features[0]
        sql_item_orders_count = '''
            select 
                b.{0} as sku,
                count(coalesce(b.{1}, 0)) as it_od_ct_1m,
                count(coalesce(b.{2}, 0)) as it_us_ct_1m
            from (select distinct sku from samples_tmp) as a
            left join
                {3} as b
            on a.sku = b.{0}
            where 
                b.{4} between '{5}' and '{6}'
            group by
                b.{0}
        '''.format(orderData['sku'], orderData['order_id'], orderData['user_id'], orderData['tableName'],
                   orderData['order_time'], start_day, end_day)
        #         print('sql_item_orders_count' + sql_item_orders_count)
        item_orders_count_df = spark.sql(sql_item_orders_count).toPandas()
        item_orders_count_df[['sku']] = item_orders_count_df[['sku']].astype(str)
        if not item_features_of_order:
            item_features_of_order = item_orders_count_df
        else:
            item_features_of_order = pd.merge(item_features_of_order, item_orders_count_df, how='left', on='sku')

        if orderData['sale_quantity']:
            # 计算近30天的sale_quantity
            sql_item_order_qty = '''
                select
                    b.{0} as sku,
                    sum(b.{1}) as it_qty_1m
                from (select distinct sku from samples_tmp) as a
                left join {2} as b
                on a.sku = b.{0}
                where b.{3} between '{4}' and '{5}'
                group by b.{0}
            '''.format(orderData['sku'], orderData['sale_quantity'], orderData['tableName'], orderData['order_time'],
                       start_day, end_day)
            #             print('sql_item_order_qty' + sql_item_order_qty)
            item_order_qty_df = spark.sql(sql_item_order_qty).toPandas()
            item_order_qty_df[['sku']] = item_order_qty_df[['sku']].astype(str)
            item_order_qty_df[['it_qty_1m']] = item_order_qty_df[['it_qty_1m']].astype(np.float)
            item_features_of_order = pd.merge(item_features_of_order, item_order_qty_df, how='left', on='sku')

        if orderData['sale_amount']:
            # 计算近30天的sale_amount
            sql_item_order_amt = '''
                select
                    b.{0} as sku,
                    sum(b.{1}) as it_amt_1m
                from (select distinct sku from samples_tmp) as a
                left join {2} as b
                on a.sku = b.{0}
                where b.{3} between '{4}' and '{5}'
                group by b.{0}
            '''.format(orderData['sku'], orderData['sale_amount'], orderData['tableName'], orderData['order_time'],
                       start_day, end_day)
            #             print('sql_item_order_amt' + sql_item_order_amt)
            item_order_amt_df = spark.sql(sql_item_order_amt).toPandas()
            item_order_amt_df[['sku']] = item_order_amt_df[['sku']].astype(str)
            item_order_amt_df[['it_amt_1m']] = item_order_amt_df[['it_amt_1m']].astype(np.float)
            item_features_of_order = pd.merge(item_features_of_order, item_order_amt_df, how='left', on='sku')

    sql_goods_list = """
                    select 
                        c.user_id, 
                        concat_ws('|', collect_set(c.sku)) as buy_item_list
                    from 
                        (select 
                            b.{0} as user_id,
                            b.{1} as sku,
                            b.{3} as sdt,
                            row_number() over (partition by b.{0} order by b.{3} desc) as ranking
                        from (select distinct user_id from samples_tmp) as a
                        left join 
                            {2} as b
                        on a.user_id = b.{0}
                        where 
                            b.{3} <= '{4}' and b.{3} >= '{5}'
                        group by
                            b.{0},
                            b.{1},
                            b.{3}) as c 
                    where 
                        c.ranking <= 10
                    group by
                        c.user_id
                """.format(orderData['user_id'], orderData['sku'], orderData['tableName'], orderData['order_time'],
                           end_day, last_x_day)
    #     print('sql_goods_list' + sql_goods_list)
    sql_goods_list = spark.sql(sql_goods_list).toPandas()
    sql_goods_list[['user_id']] = sql_goods_list[['user_id']].astype(str)
    user_features_of_order = pd.merge(user_features_of_order, sql_goods_list, how='left', on='user_id')

    if goodsData['cate']:
        sql_cate_list = """
                        select 
                            c.user_id, 
                            concat_ws('|', collect_set(d.cate)) as buy_cate_list
                        from 
                        (
                            select 
                                b.{0} as user_id,
                                b.{1} as sku,
                                b.{3} as sdt,
                                row_number() over (partition by b.{0} order by b.{3} desc) as ranking
                            from (select distinct user_id from samples_tmp) as a
                            left join
                                {2} b
                            on a.user_id = b.{0}
                            where 
                                b.{3} <= '{4}' and b.{3} >= '{5}'
                            group by
                                b.{0},
                                b.{1},
                                b.{3}) as c
                            left join 
                            (
                                select {6} as sku,
                                       {7} as cate
                                from {8}
                                where 
                                    {9} = '{4}') as d
                            on c.sku = d.sku
                        where 
                            c.ranking <= 10
                        group by
                            c.user_id
                    """.format(orderData['user_id'], orderData['sku'], orderData['tableName'], orderData['order_time'],
                               end_day, last_x_day, goodsData['sku'], goodsData['cate'], goodsData['tableName'],
                               goodsData['dt'])
        #         print('sql_cate_list' + sql_cate_list)
        cate_list_df = spark.sql(sql_cate_list).toPandas()
        cate_list_df[['user_id']] = cate_list_df[['user_id']].astype(str)
        user_features_of_order = pd.merge(user_features_of_order, cate_list_df, how='left', on='user_id')

    if goodsData['brand']:
        sql_brand_list = """
                        select 
                            c.user_id, 
                            concat_ws('|', collect_set(d.brand)) as buy_brand_list
                        from 
                        (
                            select 
                                b.{0} as user_id,
                                b.{1} as sku,
                                b.{3} as sdt,
                                row_number() over (partition by b.{0} order by b.{3} desc) as ranking
                            from (select distinct user_id from samples_tmp) as a
                            left join   {2} b
                            on a.user_id = b.{0}
                            where 
                                b.{3} <= '{4}' and b.{3} >= '{5}'
                            group by
                                b.{0},
                                b.{1},
                                b.{3}) as c
                        left join 
                        (
                            select {6} as sku,
                                   {7} as brand
                            from {8}
                            where 
                                {9} = '{4}') as d
                        on c.sku = d.sku
                        where 
                            c.ranking <= 10
                        group by
                            c.user_id
                    """.format(orderData['user_id'], orderData['sku'], orderData['tableName'], orderData['order_time'],
                               end_day, last_x_day, goodsData['sku'], goodsData['brand'], goodsData['tableName'],
                               goodsData['dt'])
        #         print("sql_brand_list:" + sql_brand_list)
        brand_list_df = spark.sql(sql_brand_list).toPandas()
        brand_list_df[['user_id']] = brand_list_df[['user_id']].astype(str)
        user_features_of_order = pd.merge(user_features_of_order, brand_list_df, how='left', on='user_id')
    return user_features_of_order, item_features_of_order


def get_bh_features(spark, is_train, samples, user_dates_features, item_dates_features, end_day, bhData, goodsData,
                    eventCode):
    last_x_days_name = ['1m']
    last_x_days = [30]
    sample_spark_df = spark.createDataFrame(samples)
    sample_spark_df.createOrReplaceTempView('samples_tmp')
    user_features_of_bh = None
    item_features_of_bh = None

    for i in range(len(user_dates_features)):
        last_x_day = user_dates_features[i]

        if eventCode['cart_add']:
            sql_user_bh = '''
                select 
                    b.{0} as user_id,
                    sum(if(b.{1} = '{9}',1,0)) as {2},
                    sum(if(b.{1} = '{10}',1,0)) as {3},
                    sum(if(b.{1} = '{11}',1,0)) as {4},
                    sum(if(b.{1} = '{10}',1,0)) / sum(if(b.{1} = '{9}',1,0)) as user_click_ratio_30d
                from (select distinct user_id from samples_tmp) as a
                left join {5} as b
                on a.user_id = b.{0}
                where
                    b.{6} between  '{7}' and '{8}'
                group by
                    b.{0}
            '''.format(bhData['user_id'], bhData['event_code'], 'user_exposure_ct_' + last_x_days_name[i],
                       'user_click_ct_' + last_x_days_name[i], 'user_shopcar_ct_' + last_x_days_name[i],
                       bhData['tableName'],
                       bhData['event_time'], last_x_day, end_day, eventCode['exposure'], eventCode['click'],
                       eventCode['cart_add'])
        else:
            sql_user_bh = '''
                select 
                    b.{0} as user_id,
                    sum(if(b.{1} = '{8}',1,0)) as {2},
                    sum(if(b.{1} = '{9}',1,0)) as {3},
                    sum(if(b.{1} = '{9}',1,0)) / sum(if(b.{1} = '{8}',1,0)) as user_click_ratio_30d
                from (select distinct user_id from samples_tmp) as a
                left join
                    {4} as b
                on a.user_id = b.{0}
                where
                    b.{5} between  '{6}' and '{7}'
                group by
                    b.{0}
            '''.format(bhData['user_id'], bhData['event_code'], 'user_exposure_ct_' + last_x_days_name[i],
                       'user_click_ct_' + last_x_days_name[i], bhData['tableName'],
                       bhData['event_time'], last_x_day, end_day, eventCode['exposure'], eventCode['click'])
        #         print("sql_user_bh:" + sql_user_bh)
        user_bh_df = spark.sql(sql_user_bh).toPandas()
        user_bh_df[['user_id']] = user_bh_df[['user_id']].astype(str)
        if not user_features_of_bh:
            user_features_of_bh = user_bh_df
        else:
            user_features_of_bh = pd.merge(user_features_of_bh, user_bh_df, how='left', on='user_id')

        sql_user_bh_exposure = '''
            select 
                b.{0} as user_id,
                count(distinct to_date(b.{1})) as {2}
            from (select distinct user_id from samples_tmp) as a
            left join
                {3} as b
            on a.user_id = b.{0}
            where
                b.{1} between  '{4}' and '{5}'
            and
                b.{6} = '{7}'
            group by 
                b.{0}
        '''.format(bhData['user_id'], bhData['event_time'], 'user_exposure_days_' + last_x_days_name[i],
                   bhData['tableName'], last_x_day, end_day, bhData['event_code'], eventCode['exposure'])
        #         print('sql_user_bh_exposure' + sql_user_bh_exposure)
        user_bh_exposure_df = spark.sql(sql_user_bh_exposure).toPandas()
        user_bh_exposure_df[['user_id']] = user_bh_exposure_df[['user_id']].astype(str)
        user_features_of_bh = pd.merge(user_features_of_bh, user_bh_exposure_df, how='left', on='user_id')

        sql_user_bh_click = '''
            select 
                b.{0} as user_id,
                count(distinct to_date(b.{1})) as {2}
            from (select distinct user_id from samples_tmp) as a
            left join
                {3} as b
            on a.user_id = b.{0}
            where
                b.{1} between  '{4}' and '{5}'
            and
                b.{6} = '{7}'
            group by 
                b.{0}
        '''.format(bhData['user_id'], bhData['event_time'], 'user_click_days_' + last_x_days_name[i],
                   bhData['tableName'], last_x_day, end_day, bhData['event_code'], eventCode['click'])
        #         print('sql_user_bh_click' + sql_user_bh_click)
        user_bh_click_df = spark.sql(sql_user_bh_click).toPandas()
        user_bh_click_df[['user_id']] = user_bh_click_df[['user_id']].astype(str)
        user_features_of_bh = pd.merge(user_features_of_bh, user_bh_click_df, how='left', on='user_id')

        if eventCode['cart_add']:
            sql_user_bh_cart_add = '''
                select 
                    b.{0} as user_id,
                    count(distinct to_date(b.{1})) as {2}
                from (select distinct user_id from samples_tmp) as a
                left join
                    {3} as b
                on a.user_id = b.{0}
                where
                    b.{1} between  '{4}' and '{5}'
                and
                    b.{6} = '{7}'
                group by 
                    b.{0}
            '''.format(bhData['user_id'], bhData['event_time'], 'user_shopcar_days_' + last_x_days_name[i],
                       bhData['tableName'], last_x_day, end_day, bhData['event_code'], eventCode['cart_add'])
            #             print('sql_user_bh_cart_add' + sql_user_bh_cart_add)
            user_bh_cart_add_df = spark.sql(sql_user_bh_cart_add).toPandas()
            user_bh_cart_add_df[['user_id']] = user_bh_cart_add_df[['user_id']].astype(str)
            user_features_of_bh = pd.merge(user_features_of_bh, user_bh_cart_add_df, how='left', on='user_id')

    sql_goods_click_list = """
                    select 
                        c.user_id, 
                        concat_ws('| ', collect_set(c.sku)) as click_item_list
                    from 
                        (select 
                            b.{0} as user_id,
                            b.{1} as sku,
                            b.{3} as sdt,
                            row_number() over (partition by b.{0} order by b.{3} desc) as ranking
                        from (select distinct user_id from samples_tmp) as a
                        left join  {2} b
                        on a.user_id = b.{0}
                        where 
                            b.{3} <= '{4}' and b.{3} >= '{5}'
                        and
                            b.{6} = '{7}'
                        group by
                            b.{0},
                            b.{1},
                            b.{3}) c
                    where 
                        c.ranking <= 10
                    group by
                        c.user_id
                """.format(bhData['user_id'], bhData['sku'], bhData['tableName'], bhData['event_time'], end_day,
                           last_x_day, bhData['event_code'], eventCode['click'])
    #     print('sql_goods_list' + sql_goods_click_list)
    goods_click_list_df = spark.sql(sql_goods_click_list).toPandas()
    goods_click_list_df[['user_id']] = goods_click_list_df[['user_id']].astype(str)
    user_features_of_bh = pd.merge(user_features_of_bh, goods_click_list_df, how='left', on='user_id')

    if goodsData['cate']:
        sql_cate_click_list = """
                        select 
                            c.user_id, 
                            concat_ws('|', collect_set(d.cate)) as click_cate_list
                        from 
                        (
                            select 
                                b.{0} as user_id,
                                b.{1} as sku,
                                b.{3} as sdt,
                                row_number() over (partition by b.{0} order by b.{3} desc) as ranking
                            from (select distinct user_id from samples_tmp) as a
                            left join
                                {2} as b
                            on a.user_id = b.{0}
                            where 
                                b.{3} <= '{4}' and b.{3} >= '{5}'
                            and 
                                b.{6} = '{11}'
                            group by
                                b.{0},
                                b.{1},
                                b.{3}) as c
                        left join 
                        (
                            select {7} as sku,
                                   {8} as cate
                            from {9}
                            where 
                                {10} = '{4}') as d
                        on c.sku = d.sku
                        where 
                            c.ranking <= 10
                        group by
                            c.user_id
                    """.format(bhData['user_id'], bhData['sku'], bhData['tableName'], bhData['event_time'],
                               end_day, last_x_day, bhData['event_code'], goodsData['sku'], goodsData['cate'],
                               goodsData['tableName'], goodsData['dt'], eventCode['click'])
        #         print('sql_cate_click_list' + sql_cate_click_list)
        cate_click_list_df = spark.sql(sql_cate_click_list).toPandas()
        cate_click_list_df[['user_id']] = cate_click_list_df[['user_id']].astype(str)
        user_features_of_bh = pd.merge(user_features_of_bh, cate_click_list_df, how='left', on='user_id')

    if goodsData['brand']:
        sql_brand_click_list = """
                        select 
                            c.user_id, 
                            concat_ws('|', collect_set(d.brand)) as click_brand_list
                        from 
                        (
                            select 
                                b.{0} as user_id,
                                b.{1} as sku,
                                b.{3} as sdt,
                                row_number() over (partition by b.{0} order by b.{3} desc) as ranking
                            from (select distinct user_id from samples_tmp) as a
                            left join
                                {2} b
                            on a.user_id = b.{0}
                            where 
                                b.{3} <= '{4}' and b.{3} >= '{5}'
                            and
                                b.{6} = '{11}'
                            group by
                                b.{0},
                                b.{1},
                                b.{3}) as c
                        left join 
                        (
                            select {7} as sku,
                                   {8} as brand
                            from {9}
                            where 
                                {10} = '{4}') as d
                        on c.sku = d.sku
                        where 
                            c.ranking <= 10
                        group by
                            c.user_id
                    """.format(bhData['user_id'], bhData['sku'], bhData['tableName'], bhData['event_time'],
                               end_day, last_x_day, bhData['event_code'], goodsData['sku'], goodsData['brand'],
                               goodsData['tableName'],
                               goodsData['dt'], eventCode['exposure'])
        #         print('sql_brand_click_list' + sql_brand_click_list)
        brand_click_list_df = spark.sql(sql_brand_click_list).toPandas()
        brand_click_list_df[['user_id']] = brand_click_list_df[['user_id']].astype(str)
        user_features_of_bh = pd.merge(user_features_of_bh, brand_click_list_df, how='left', on='user_id')

    if is_train:
        start_day = item_dates_features[0]
        if eventCode['cart_add']:
            sql_item_bh = '''
                        select 
                            b.{0} as sku,
                            sum(if(b.{1} = '{9}',1,0)) as {2},
                            sum(if(b.{1} = '{10}',1,0)) as {3},
                            sum(if(b.{1} = '{11}',1,0)) as {4},
                            sum(if(b.{1} = '{10}',1,0)) / sum(if(b.{1} = '{9}',1,0)) as item_click_ratio_1m
                        from (select distinct sku from samples_tmp) as a
                        left join
                            {5} as b
                        on a.sku = b.{0}
                        where
                            b.{6} between  '{7}' and '{8}'
                        group by
                            b.{0}
                    '''.format(bhData['sku'], bhData['event_code'], 'item_exposure_ct_' + last_x_days_name[i],
                               'item_click_ct_' + last_x_days_name[i], 'item_shopcar_ct_' + last_x_days_name[i],
                               bhData['tableName'],
                               bhData['event_time'], last_x_day, end_day, eventCode['exposure'], eventCode['click'],
                               eventCode['cart_add'])
        else:
            sql_item_bh = '''
                        select 
                            b.{0} as sku,
                            sum(if(b.{1} = '{8}',1,0)) as {2},
                            sum(if(b.{1} = '{9}',1,0)) as {3},
                            sum(if(b.{1} = '{9}',1,0)) / sum(if(b.{1} = '{8}',1,0)) as item_click_ratio_1m
                        from (select distinct sku from samples_tmp) as a
                        left join
                            {4} as b
                        on a.sku = b.{0}
                        where
                            b.{5} between  '{6}' and '{7}'
                        group by
                            b.{0}
                    '''.format(bhData['sku'], bhData['event_code'], 'item_exposure_ct_' + last_x_days_name[i],
                               'item_click_ct_' + last_x_days_name[i], bhData['tableName'],
                               bhData['event_time'], last_x_day, end_day, eventCode['exposure'], eventCode['click'])
        #         print("sql_item_bh:" + sql_item_bh)
        item_bh_df = spark.sql(sql_item_bh).toPandas()
        item_bh_df[['sku']] = item_bh_df[['sku']].astype(str)
        if not item_features_of_bh:
            item_features_of_bh = item_bh_df
        else:
            item_features_of_bh = pd.merge(item_features_of_bh, item_bh_df, how='left', on='sku')

    return user_features_of_bh, item_features_of_bh


def get_user_features(spark, samples, userData, cur_str):
    '''
    从用户表获取用户特征
    :param spark: spark连接对象
    :param samples: 样本
    :param userData['tableName: 用户表名称
    :param userData: 用户表字段映射
    :param cur_str: 当天日期
    :return: 样本用户特征
    '''
    sample_spark_df = spark.createDataFrame(samples)
    sample_spark_df.createOrReplaceTempView('samples_tmp')
    userlabel_df = None
    # 待改进：可合并
    if userData['sex']:
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
        '''.format(userData['user_id'], userData['sex'], userData['tableName'], userData['dt'], cur_str)
        #         print('sql_gender' + sql_gender)
        gender_df = spark.sql(sql_gender).toPandas()
        gender_df[['user_id', 'gender']] = gender_df[['user_id', 'gender']].astype(str)
        if userlabel_df is None:
            userlabel_df = gender_df
        else:
            userlabel_df = pd.merge(userlabel_df, gender_df, how='left', on='user_id')

    if userData['age']:
        sql_age = '''
            select 
                a.user_id,
                b.age
            from
                (select distinct user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as age 
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id
        '''.format(userData['user_id'], userData['age'], userData['tableName'], userData['dt'], cur_str)
        #         print('sql_age' + sql_age)
        age_df = spark.sql(sql_age).toPandas()
        age_df[['user_id']] = age_df[['user_id']].astype(str)
        #         age_df[['age']] = age_df[['age']].astype(np.int64)
        if userlabel_df is None:
            userlabel_df = age_df
        else:
            userlabel_df = pd.merge(userlabel_df, age_df, how='left', on='user_id')

    if userData['life_stage']:
        sql_life_stage = '''
            select 
                a.user_id,
                b.life_stage
            from
                (select distinct user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as life_stage
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id
        '''.format(userData['user_id'], userData['life_stage'], userData['tableName'], userData['dt'], cur_str)
        #         print('sql_life_stage' + sql_life_stage)
        sql_life_stage = spark.sql(sql_life_stage).toPandas()
        sql_life_stage[['user_id', 'life_stage']] = sql_life_stage[['user_id', 'life_stage']].astype(str)
        if userlabel_df is None:
            userlabel_df = sql_life_stage
        else:
            userlabel_df = pd.merge(userlabel_df, sql_life_stage, how='left', on='user_id')

    if userData['consume_level']:
        sql_consume_lvl = '''
            select 
                a.user_id,
                b.consume_lvl
            from
                (select distinct user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as consume_lvl 
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id
        '''.format(userData['user_id'], userData['consume_level'], userData['tableName'], userData['dt'], cur_str)
        #         print('sql_consume_lvl' + sql_consume_lvl)
        consume_lvl_df = spark.sql(sql_consume_lvl).toPandas()
        consume_lvl_df[['user_id', 'consume_lvl']] = consume_lvl_df[['user_id', 'consume_lvl']].astype(str)
        if userlabel_df is None:
            userlabel_df = consume_lvl_df
        else:
            userlabel_df = pd.merge(userlabel_df, consume_lvl_df, how='left', on='user_id')

    if userData['online_signup_time']:
        sql_sign_on = '''
            select 
                a.user_id,
                b.sign_on_days
            from
                (select distinct user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    if({1} is null, 0, datediff('{2}',to_date({1}))) as sign_on_days 
                from {4}
                where {3} = '{2}' ) as b
            on a.user_id = b.user_id
        '''.format(userData['user_id'], userData['online_signup_time'], cur_str, userData['dt'], userData['tableName'])
        #         print('sql_sign_on' + sql_sign_on)
        sign_on_df = spark.sql(sql_sign_on).toPandas()
        sign_on_df[['user_id']] = sign_on_df[['user_id']].astype(str)
        #         sign_on_df[['sign_on_days']] = sign_on_df[['sign_on_days']].astype(np.int64)
        if userlabel_df is None:
            userlabel_df = sign_on_df
        else:
            userlabel_df = pd.merge(userlabel_df, sign_on_df, how='left', on='user_id')

    if userData['recent_view_day']:
        sql_latest_view = '''
            select 
                a.user_id,
                b.latest_view_days
            from
                (select distinct user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    if({1} is null, 0, datediff('{2}', to_date({1}))) as latest_view_days 
                from {4}
                where {3} = '{2}' ) as b
            on a.user_id = b.user_id
        '''.format(userData['user_id'], userData['recent_view_day'], cur_str, userData['dt'], userData['tableName'])
        #         print('sql_latest_view' + sql_latest_view)
        latest_view_df = spark.sql(sql_latest_view).toPandas()
        latest_view_df[['user_id']] = latest_view_df[['user_id']].astype(str)
        #         latest_view_df[['latest_view_days']] = latest_view_df[['latest_view_days']].astype(np.int64)
        if userlabel_df is None:
            userlabel_df = latest_view_df
        else:
            userlabel_df = pd.merge(userlabel_df, latest_view_df, how='left', on='user_id')

    if userData['city']:
        sql_city = '''
            select 
                a.user_id,
                b.city
            from
                (select distinct user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as city 
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id
        '''.format(userData['user_id'], userData['city'], userData['tableName'], userData['dt'], cur_str)
        #         print('sql_city' + sql_city)
        city_df = spark.sql(sql_city).toPandas()
        city_df[['user_id', 'city']] = city_df[['user_id', 'city']].astype(str)
        if userlabel_df is None:
            userlabel_df = city_df
        else:
            userlabel_df = pd.merge(userlabel_df, city_df, how='left', on='user_id')

    if userData['province']:
        sql_province = '''
            select 
                a.user_id,
                b.province
            from
                (select distinct user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as province 
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id
        '''.format(userData['user_id'], userData['province'], userData['tableName'], userData['dt'], cur_str)
        #         print('sql_province' + sql_province)
        province_df = spark.sql(sql_province).toPandas()
        province_df[['user_id', 'province']] = province_df[
            ['user_id', 'province']].astype(str)
        if userlabel_df is None:
            userlabel_df = province_df
        else:
            userlabel_df = pd.merge(userlabel_df, province_df, how='left', on='user_id')

    if userData['membership_level']:
        sql_membership_level = '''
            select 
                a.user_id,
                b.membership_level
            from
                (select distinct user_id from samples_tmp) as a
            left join
                (select 
                    {0} as user_id,
                    {1} as membership_level 
                from {2}
                where {3} = '{4}' ) as b
            on a.user_id = b.user_id
        '''.format(userData['user_id'], userData['membership_level'], userData['tableName'], userData['dt'], cur_str)
        #         print('sql_membership_level' + sql_membership_level)
        membership_level_df = spark.sql(sql_membership_level).toPandas()
        membership_level_df[['user_id', 'membership_level']] = membership_level_df[
            ['user_id', 'membership_level']].astype(str)
        if userlabel_df is None:
            userlabel_df = membership_level_df
        else:
            userlabel_df = pd.merge(userlabel_df, membership_level_df, how='left', on='user_id')

    # 预留字段
    # if userData['预留字段']:
    #     sql_user_tag = '''
    #         select
    #             a.user_id,
    #             b.user_tag
    #         from
    #             (select distinct user_id from samples_tmp) as a
    #         left join
    #             (select
    #                 {0} as user_id,
    #                 {1} as user_tag
    #             from {2}
    #             where {3} = '{4}' ) as b
    #         on a.user_id = b.user_id
    #     '''.format(userData['user_id'], userData['预留字段'], userData['tableName'], userData['dt'], cur_str)
    #     # print('sql_user_tag' + sql_user_tag)
    #     sql_user_tag = spark.sql(sql_user_tag).toPandas()
    #     sql_user_tag[['user_id', 'user_tag']] = sql_user_tag[
    #         ['user_id', 'user_tag']].astype(str)
    #     if not userlabel_df:
    #         userlabel_df = sql_user_tag
    #     else:
    #         userlabel_df = pd.merge(userlabel_df, sql_user_tag, how='outer', on='user_id')

    return userlabel_df


def get_item_features(spark, samples, goodsData, cur_str):
    '''
    从商品表获取商品特征
    :param spark: spark连接对象
    :param samples: 样本
    :param goodsData['tableName: 商品表名称
    :param goodsData: 商品表字段映射
    :param cur_str: 当天日期
    :return: 样本商品特征
    '''
    sample_spark_df = spark.createDataFrame(samples)
    sample_spark_df.createOrReplaceTempView('samples_tmp')
    itemlabel_df = None

    if goodsData['cate']:
        sql_cate = '''
            select 
                a.sku,
                b.cate
            from
                (select distinct sku from samples_tmp) as a
            left join
                (select 
                    {0} as sku,
                    {1} as cate 
                from {2}
                where {3} = '{4}') as b
            on a.sku = b.sku
        '''.format(goodsData['sku'], goodsData['cate'], goodsData['tableName'], goodsData['dt'], cur_str)
        #         print('sql_cate' + sql_cate)
        cate_df = spark.sql(sql_cate).toPandas()
        cate_df[['sku', 'cate']] = cate_df[['sku', 'cate']].astype(str)
        if itemlabel_df is None:
            itemlabel_df = cate_df
        else:
            itemlabel_df = pd.merge(itemlabel_df, cate_df, how='left', on='sku')

    if goodsData['brand']:
        sql_brand = '''
             select 
                 a.sku,
                 b.brand
             from
                 (select distinct sku from samples_tmp) as a
             left join
                 (select 
                     {0} as sku,
                     {1} as brand 
                 from {2}
                 where {3} = '{4}') as b
             on a.sku = b.sku
         '''.format(goodsData['sku'], goodsData['brand'], goodsData['tableName'], goodsData['dt'], cur_str)
        brand_df = spark.sql(sql_brand).toPandas()
        brand_df[['sku', 'brand']] = brand_df[['sku', 'brand']].astype(str)
        if itemlabel_df is None:
            itemlabel_df = brand_df
        else:
            itemlabel_df = pd.merge(itemlabel_df, brand_df, how='left', on='sku')

    # if goodsData['其他商品标签']:
    #     sql_item_tag = '''
    #          select
    #              a.sku,
    #              b.item_tag
    #          from
    #              (select distinct sku from samples_tmp) as a
    #          left join
    #              (select
    #                  {0} as sku,
    #                  {1} as item_tag
    #              from {2}
    #              where {3} = '{4}' ) as b
    #          on a.sku = b.sku
    #      '''.format(goodsData['sku'], goodsData['brand'], goodsData['tableName'], goodsData['dt'], cur_str)
    #     print('sql_item_tag' + sql_item_tag)
    #     # item_tag_df = spark.sql(sql_item_tag).toPandas()
    #     # item_tag_df[['sku', 'item_tag']] = item_tag_df[['sku', 'item_tag']].astype(str)
    #     # if not itemlabel_df:
    #     #     itemlabel_df = item_tag_df
    #     # else:
    #     #     itemlabel_df = pd.merge(itemlabel_df, item_tag_df, how='outer', on='sku')

    return itemlabel_df


def crowd_filter(spark, filter_condition, userData, cur_str):
    sql_crowd_filter = '''
            select
                distinct {0} as user_id
            from
                {1}
            where 
                {2} = '{3}'
            and 
                {4}
        '''.format(userData['user_id'], userData['tableName'], userData['dt'], cur_str, filter_condition)
    crowd_filter = spark.sql(sql_crowd_filter).toPandas()
    return crowd_filter


def crowd_select_scence(spark, samples, orderData, goodsData, cur_str, scene, expansion_dimension):
    sample_spark_df = spark.createDataFrame(samples)
    sample_spark_df.createOrReplaceTempView('samples_tmp')

    if expansion_dimension['goodsDimensionType'] == "0":
        expansion_sql = """
                    select
                        a.user_id,
                        case 
                            when c.dif_days is null then 1
                            when c.dif_days>=0 and c.dif_days<=180 then 2
                            when c.dif_days>180 and c.dif_days<= 365 then 3
                            else 4 end as type
                    from (select distinct user_id from samples_tmp) as a
                    left join
                    (   select
                            {0} as user_id,
                            datediff("{1}",max(to_date({2}))) as dif_days
                        from
                            {3} as b
                        group by 
                            {0}) as c
                    on a.user_id = c.user_id  
                """.format(orderData['user_id'], cur_str, orderData['order_time'], orderData['tableName'])

    elif expansion_dimension['goodsDimensionType'] == "1":
        goods_value_list = ','.join(expansion_dimension['goodsDimensionValue'])
        expansion_sql = """
            select
                a.user_id,
                case 
                    when d.dif_days is null then 1
                    when d.dif_days>=0 and d.dif_days<=180 then 2
                    when d.dif_days>180 and d.dif_days<= 365 then 3
                    else 4 end as type
            from (select distinct user_id from samples_tmp) as a
            left join
            (   select
                    {0} as user_id,
                    datediff("{1}",max(to_date({2}))) as dif_days
                from
                    {3} as b 
                left join 
                (   select {4} as sku,
                           {5} as brand
                    from {6}
                    where 
                        {7} = '{1}') as c
                on b.{8} = c.sku
                where
                    c.brand in ('{9}')
                group by 
                    {0}) as d
            on a.user_id = d.user_id  
        """.format(orderData['user_id'], cur_str, orderData['order_time'], orderData['tableName'], goodsData['sku'],
                   goodsData['brand'], goodsData['tableName'], goodsData['dt'], orderData['sku'], goods_value_list)

    elif expansion_dimension['goodsDimensionType'] == "2":
        goods_value_list = ','.join(expansion_dimension['goodsDimensionValue'])
        expansion_sql = """
                    select
                        a.user_id,
                        case 
                            when d.dif_days is null then 1
                            when d.dif_days>=0 and d.dif_days<=180 then 2
                            when d.dif_days>180 and d.dif_days<= 365 then 3
                            else 4 end as type
                    from (select distinct user_id from samples_tmp) as a
                    left join
                    (   select
                            {0} as user_id,
                            datediff("{1}",max(to_date({2}))) as dif_days
                        from
                            {3} as b 
                        left join 
                        (   select {4} as sku,
                                   {5} as cate
                            from {6}
                            where 
                                {7} = '{1}') as c
                        on b.{8} = c.sku
                        where
                            c.cate in ('{9}')
                        group by 
                            {0}) as d
                    on a.user_id = d.user_id  
                """.format(orderData['user_id'], cur_str, orderData['order_time'], orderData['tableName'],
                           goodsData['sku'],
                           goodsData['cate'], goodsData['tableName'], goodsData['dt'], orderData['sku'],
                           goods_value_list)
    else:
        raise ("未知参数类型")
    expansion_sql_df = spark.sql(expansion_sql).toPandas()
    expansion_sql_df[['user_id']] = expansion_sql_df[['user_id']].astype(str)

    if scene == "2":
        crowd = expansion_sql_df[expansion_sql_df['type'] == 1]
    elif scene == "3":
        crowd = expansion_sql_df[expansion_sql_df['type'] == 2]
    elif scene == "4":
        crowd = expansion_sql_df[expansion_sql_df['type'] == 3]
    elif scene == "5":
        crowd = expansion_sql_df[expansion_sql_df['type'] == 4]
    else:
        raise ("未知参数类型")
    return crowd


def upload_user_embedding(spark, taskid, user_embedding_df):
    sql_lastest_version = '''
        select
            max(version)
        from
            algorithm.lookalike_cdp_result
        where
            taskid = '{0}'
    '''.format(taskid)
    lastest_version_df = spark.sql(sql_lastest_version).toPandas()
    if len(lastest_version_df) == 0 or lastest_version_df.iloc[0, 0] is None or math.isnan(lastest_version_df.iloc[0,0]):
        cur_version = 1
    else:
        cur_version = int(lastest_version_df.iloc[0, 0]) + 1

    embedding_spark_df = spark.createDataFrame(user_embedding_df)
    embedding_spark_df.createOrReplaceTempView('user_embedding_tmp')

    sql_result = '''
        insert into algorithm.lookalike_cdp_result
        select 
            user_id,
            embedding,
            '{0}' as taskid,
            {1} as version
        from
            user_embedding_tmp
    '''.format(taskid, cur_version)
    spark.sql(sql_result)


def download_user_embedding(spark, taskid):
    sql_user_embedding = '''
        select 
            user_id, 
            embedding 
        from algorithm.lookalike_cdp_result as a,
        (
            select 
                taskid, 
                max(version) as lastest_version 
            from algorithm.lookalike_cdp_result where taskid = '{0}'
            group by taskid) as b
        where 
            a.taskid = b.taskid
        and
            a.version = b.lastest_version
    '''.format(taskid)
    user_embedding_df = spark.sql(sql_user_embedding).toPandas()
    return user_embedding_df

def upload(filepath, taskId):
    cli = pyhdfs.HdfsClient(hosts="{}:{}".format(hdfs_config['host'],hdfs_config['port']))
    target_file_path = os.path.join('hdfs:///user/ai/cdp/lookalike/parameter', str(taskId) + '.txt')
    if cli.exists(target_file_path):
        cli.delete(target_file_path)
    cli.copy_from_local(filepath, target_file_path)
    return target_file_path

