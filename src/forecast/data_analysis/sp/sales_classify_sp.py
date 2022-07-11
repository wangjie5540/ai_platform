# -*- coding: utf-8 -*-
# @Time : 2022/12/25
# @Author : Arvin
from forecast.common.common_helper import *

def compute_tail_sku(sparkdf_sales, col_qty, col_key, w_tail, col_time, threshold_count, threshold_proportion):
    windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w_tail - 1),
                                                                                    end=Window.currentRow)
    # 计算过去w天的销量总和
    sparkdf_sales = sparkdf_sales.withColumn('s_wm', psf.sum(col_qty).over(windowOpt))
    sparkdf_sales_days = sparkdf_sales.groupBy(col_key).count()
    sparkdf_sales_days = sparkdf_sales_days.withColumnRenamed('count', 'count_all')
    sparkdf_sales_threshold = sparkdf_sales.filter(sparkdf_sales['s_wm'] > threshold_count).groupBy(col_key).count()
    sparkdf_sales_threshold = sparkdf_sales_threshold.withColumnRenamed('count', 'threshold_count')
    sales_data_tail = sparkdf_sales_days.join(sparkdf_sales_threshold, on=col_key, how='left')
    sales_data_tail = sales_data_tail.fillna(0)  # null值进行补0
    sales_data_tail = sales_data_tail.withColumn(
        "category_tail",
        psf.when(sales_data_tail.threshold_count / sales_data_tail.count_all < threshold_proportion, 'tail').otherwise(
            None))
    sales_data_tail = sales_data_tail.drop('threshold_count').drop('count_all')
    return sales_data_tail


def compute_ls_sku(sparkdf_sales, col_qty, col_key, w_ls, col_time, edate, threshold_sales):
    # 计算过去N天的销量
    spark_sales_sum = sparkdf_sales.filter(
        "{1} >  to_unix_timestamp( '{2}','yyyyMMdd') - {0}".format(days(w_ls - 1), col_time, edate)).groupby(
        col_key).agg(psf.sum(psf.col(col_qty)).alias("qty_sum"))
    # 计算去年同期的销量
    spark_sales_sum_last = sparkdf_sales.filter(
        "{2} > '{3}' - {0} and {2} <= '{3}' - {1}".format(days(365 + w_ls), days(365), col_time, edate)).groupby(
        col_key).agg(psf.sum(psf.col(col_qty)).alias("qty_sum_last"))
    spark_sales_sum = spark_sales_sum.join(spark_sales_sum_last, on=col_key, how='left')
    spark_sales_sum = spark_sales_sum.fillna(0)
    spark_sales_sum = spark_sales_sum.withColumn(
        "category_ls",
        psf.when((spark_sales_sum.qty_sum < threshold_sales) & (spark_sales_sum.qty_sum_last < threshold_sales),
                 'ls').otherwise(None))

    return spark_sales_sum


def get_sku_classfication_info(sparkdf_sales, col_qty, col_key, w_tail, w_ls, edate, col_time, threshold_count,
                               threshold_proportion, threshold_sales, col_sku_category):
    # 1.计算尾部
    sparkdf_tail = compute_tail_sku(sparkdf_sales, col_qty, col_key, w_tail, col_time, threshold_count, threshold_proportion)
    # 2.计算低销
    sparkdf_ls = compute_ls_sku(sparkdf_sales, col_qty, col_key, w_ls, col_time, edate, threshold_sales)

    sparkdf_tail = sparkdf_tail.join(sparkdf_ls, on=col_key, how='left')
    sparkdf_tail = sparkdf_tail.withColumn(
        "{0}".format(col_sku_category), psf.when(sparkdf_tail.category_tail.isNotNull(), sparkdf_tail.category_tail)
            .when(sparkdf_tail.category_ls.isNotNull(), sparkdf_tail.category_ls)
            .otherwise('normal'))

    #     #3.汇总
    #     goods_class_table()
    return sparkdf_tail


def sales_classify(spark, param):
    """订单过滤
       col_key：主键
       filter_func:过滤函数
       sdate:开始时间 '20210101'
       edate:结束时间
       col_qty:过滤字段
       w:时间窗口
       conn:函数之间关系
       col_time:时间戳字段
    """
    col_key = param['col_key']
    w_tail = param['w_tail']
    w_ls = param['w_ls']
    col_time = param['col_time']
    col_qty = param['col_qty']
    threshold_count = param['threshold_count']
    edate = param['edate']
    threshold_proportion = param['threshold_proportion']
    threshold_sales = param['threshold_sales']
    col_sku_category = param['col_sku_category']
    input_table = param['input_table']
    output_table = param['output_table']
    input_partition_name = param['input_partition_name']
    partition_list = param['partition_list']
    output_partition_name = param['output_partition_names']
    sparkdf_sales = read_table(spark, input_table, sdt='N', partition_name=input_partition_name, partition_list=partition_list)
    #1.读表、存表按照partition_name\partition_list
    #2.新创建数据分析文件夹，将销量分层的规则独立出来
    #3.代码合并 以及一些通用的common抽取
    sparkdf_classfiy = get_sku_classfication_info(sparkdf_sales, col_qty, col_key, w_tail, w_ls, edate, col_time,
                                                      threshold_count, threshold_proportion, threshold_sales,
                                                      col_sku_category)
    save_table(spark, sparkdf_classfiy, output_table, partition=output_partition_name)

    return 'SUCCESS'