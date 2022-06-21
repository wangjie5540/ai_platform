# -*- coding:utf-8  -*-
"""
Copyright (c) 2021-2022 北京数势云创科技有限公司 <http://www.digitforce.com>
All rights reserved. Unauthorized reproduction and use are strictly prohibited
include:
    价格弹性模型的数据预处理
"""
import os
from pyspark.sql.functions import isnull
from pyspark.sql import functions as F
from pyspark.sql import Window
from components.common.toml_helper import TomlOperation
from components.data_process.runnable import sales_filter
from components.common.save_data import is_exist_table, write_to_hive

from components.price_elasticity.common.common import *


def load_params(file_path):
    """运行run方法时 参数给定在程序中还是传入???"""
    param_cur = {
        'mode_type': 'sp',
        'sdate': '20220101',
        'edate': '20220501'
    }
    f = TomlOperation(file_path + "/config/param.toml")
    params_all = f.read_file()
    # 获取项目1配置参数
    params = params_all['price_elasticity']
    params.update(param_cur)
    return params


def get_predict_result(spark, predict_result_table):
    """
    目的：获取预测数据，如果没有则预测结果或者预测时长不够，则需要调用这部分api
    """
    if is_exist_table(spark, predict_result_table) == '':
        pass
    else:
        pass
    sparkdf = spark.table(predict_result_table).select()
    return sparkdf


def get_filter_data(spark, sales_filter_table, param):
    """
    目的：获取销量还原后的数据，如果没有则需要调用这部分api
    """
    filter_table_col = param['filter_table_col']
    if is_exist_table(spark, sales_filter_table) == '':
        pass
    else:

        sales_filter.run()

    sparkdf = spark.table(sales_filter_table).select(filter_table_col).filter("y_type='c'")
    return sparkdf


def data_prepare(spark):
    """
    样本选择
    :param spark:spark
    :param param: 选择样本参数
    :return: 样本
    """
    file_path = os.getcwd()
    params = load_params(file_path)
    # dict_rename = params['dict_rename']
    dict_rename = {"site_code": "shop_id", "goods_code": "goods_id", "quantity": "qty", "paid_amount": "sale_amt",
                   "dt": "dt"}

    # ==对应原始order表中相应的字段==
    ori_shop_exp = params['shop_exp']
    ori_goods_exp = params['goods_exp']
    ori_qty_exp = params['qty_exp']
    ori_amt_exp = params['paid_amount']
    ori_dt = params['ori_dt']
    # ==重命名之后的统一的列明（标品中的列明）==
    col_qty = params['col_qty']
    col_sale_amt = params['col_sale_amt']
    dt = 'dt'
    col_key = params['col_key']
    partition_key = params['partition_key']

    w = params['w']
    # 原始订单表
    order_table = params['order_table']
    # 销量还原表
    filter_table = params['filter_table']

    filter_table_col_y = params['filter_table_col_y']
    # =======
    sum_qty = "sum" + '_' + col_qty
    sum_sale_amt = "sum" + '_' + col_sale_amt
    # 读取订单表销售数据
    sparkdf_order = spark.table(order_table). \
        select([ori_shop_exp, ori_goods_exp, ori_qty_exp, ori_amt_exp, ori_dt]).filter(
        ori_qty_exp + ">0")  # ~isnull('quantity')
    # 对读取的原始销量表重命名，命名成标品标准的字段
    sparkdf_order = rename_columns(sparkdf_order, dict_rename)
    # 处理价格
    spark_price = sparkdf_order.groupby(col_key).agg({col_qty: 'sum', col_sale_amt: 'sum'}) \
        .withColumnRenamed("sum" + "(%s)" % col_qty, sum_qty) \
        .withColumnRenamed("sum" + "(%s)" % col_sale_amt, "sum" + '_' + col_sale_amt)
    spark_price = spark_price.withColumn("price",
                                         spark_price["sum" + '_' + col_sale_amt] / spark_price[sum_qty])

    # todo:如果没有过滤的hive表，是否需要重新调用这部分清洗的代码
    # 读取销量还原的数据，如果销量还原表不存在则调用销量还原的api
    sparkdf_y = get_filter_data(spark, filter_table, params)
    # 关联两张表的数据
    df = spark_price.join(sparkdf_y, col_key, 'outer').filter(~isnull(filter_table_col_y))

    df = df.withColumn("ts", F.unix_timestamp(F.to_timestamp(F.col(dt), 'yyyyMMdd'), "format='yyyy-MM-dd"))
    # 对销量填0
    df = df.fillna(0, subset=[sum_qty])

    windowOpt = Window.partitionBy(partition_key).orderBy(F.col('ts')).rangeBetween(start=-days(w - 1),
                                                                                    end=Window.currentRow)
    df = df.withColumn("{0}_{1}_{2}".format('price', 'max', w),
                       F.max(F.col("{0}".format('price'))).over(windowOpt))
    # 滚动计算销量的求和
    df = df.withColumn("{0}_{1}_{2}".format(sum_qty, 'sum', w),
                       F.sum(F.col("{0}".format(sum_qty))).over(windowOpt))
    # 滚动计算销售额的求和
    df = df.withColumn("{0}_{1}_{2}".format(sum_sale_amt, 'sum', w),
                       F.sum(F.col("{0}".format(sum_sale_amt))).over(windowOpt))
    # 以此计算价格的均值
    df = df.withColumn("price_mean", F.col("{0}_{1}_{2}".format(sum_sale_amt, 'sum', w)) / F.col(
        "{0}_{1}_{2}".format(sum_qty, 'sum', w)))
    # 根据是取价格均值还是最大值进行判断，并替换原price列
    df = df.withColumn("price",
                       (F.when((F.col(filter_table_col_y) > 0) & (F.col(sum_qty) == 0), F.col('price_mean')).when(
                           (F.col(filter_table_col_y) == 0) & (F.col(sum_qty) == 0),
                           F.col("{0}_{1}_{2}".format('price', 'max', w))).otherwise(
                           F.col("price"))))
    # 对价格的来源进行打标签
    df = df.withColumn("label_price",
                       (F.when((F.col(filter_table_col_y) > 0) & (F.col(sum_qty) == 0), F.lit('mean')).when(
                           (F.col(filter_table_col_y) == 0) & (F.col(sum_qty) == 0), F.lit('max')).otherwise(
                           F.lit("ori"))))

    # 最后将真实销量用理论销量来代替
    df = df.withColumn("label_qty",
                       F.when((F.col(sum_qty) < F.col(filter_table_col_y)) | (F.col(sum_qty) == 0),
                              F.lit('adjust')).otherwise(
                           F.lit('ori')))
    df = df.withColumn(sum_qty,
                       F.when(F.col(sum_qty) < F.col(filter_table_col_y), F.col(filter_table_col_y)).otherwise(
                           F.col(sum_qty)))
    # 选取需要的字段名称
    df = df.select(['shop_id', 'goods_id', 'dt', sum_qty, 'price', 'label_price', 'label_qty'])
    # 写入表中
    write_to_hive(spark, df, 'dt', 'ai_dm_dev.price_elastic_mid_d', 'overwrite')
    return df
