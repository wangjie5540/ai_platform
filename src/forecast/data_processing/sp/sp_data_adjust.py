# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin

from forecast.common.common_helper import *
import os
import sys



def adjust_by_interpolate(df, start_date, end_date, col_qty, method='linear'):
    """插值填补"""
    c_columns = df.columns
    #     df = sales_continue(df, start_date, end_date,col_qty)
    df[col_qty] = df[col_qty].interpolate(method, limit_direction='both')
    return df[c_columns]


def adjust_by_stat(df, start_date, end_date, col_qty, w, agg_func='mean'):
    """过去时间窗口的统计量填补 w:天数 agg_func:mean/median/mode"""
    c_columns = df.columns
    #     df = sales_continue(df, start_date, end_date,col_qty)
    df['corr_qty'] = df[col_qty].rolling(window=w, min_periods=0).agg(agg_func)
    df[col_qty] = df[[col_qty, 'corr_qty']].apply(lambda x: x[0] if not pd.isna(x[0]) else x[1], axis=1)
    return df[c_columns]


def sales_fill(qty_value, openinv_value, fill_value):
    """销售填充:有库存无销售填充fill_value"""
#     return qty_value
    if (qty_value is None or pd.isna(qty_value)) and (openinv_value is not None or not pd.isna(openinv_value)) and openinv_value > fill_value:
        return fill_value
    else:
        return qty_value


def adjust_by_column(sales_sparkdf, stock_sparkdf, join_key, col_openinv, col_qty, sdate, edate, fill_value):
    """销售补零 要不要把销售连续起来？"""
   
    sparkdf = stock_sparkdf.join(sales_sparkdf, on=join_key, how='left')
    sparkdf = sparkdf.withColumn("fill_tmp", psf.lit(fill_value))
    sales_fill_udf = udf(sales_fill, DoubleType())
    sparkdf = sparkdf.withColumn("fill_{}".format(col_qty),sales_fill_udf(sparkdf[col_qty], sparkdf[col_openinv], sparkdf.fill_tmp))
    return sparkdf.filter(date_filter_condition(sdate, edate))    



def adjust_by_bound(sparkdf, col_key, col_boxcox, col_qty, filter_func, sdate, edate, replace_func, w=90, conn='and',
                    col_time='sdt'):
    """
    过去一段时间窗口w内 ，通过filter_func超出上下限的用replace_func的边界值替换
    """
    windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w),
                                                                                    end=Window.currentRow)
    for dict_key in filter_func:
        func_up, func_low = globals()[dict_key](windowOpt, col_boxcox, filter_func[dict_key])
        sparkdf = sparkdf.withColumn("{}_up".format(dict_key), func_up)
        sparkdf = sparkdf.withColumn("{}_low".format(dict_key), func_low)
    filter_up_str, filter_low_str = "1=1 ", "1=1 "
    if conn == "and":
        for dict_key in filter_func:
            filter_up_str += "and {0}>{1}_up ".format(col_boxcox, dict_key)
            filter_low_str += "and {0}<{1}_low ".format(col_boxcox, dict_key)
    else:
        for dict_key in filter_func:
            filter_up_str += "or {0}>{1}_up ".format(col_boxcox, dict_key)
            filter_low_str += "or {0}<{1}_low ".format(col_boxcox, dict_key)

    for dict_key in replace_func:
        func_up, func_low = globals()[dict_key](windowOpt, col_qty, replace_func[dict_key])
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w),
                                                                                        end=Window.currentRow)
        sparkdf = sparkdf.withColumn("{}_replace_up".format(dict_key), func_up)
        sparkdf = sparkdf.withColumn("{}_replace_low".format(dict_key), func_low)
        sparkdf = sparkdf.withColumn(col_qty, psf.when(psf.expr(filter_up_str),
                                                       sparkdf['{}_replace_up'.format(dict_key)]).otherwise(
            sparkdf[col_qty]))
        sparkdf = sparkdf.withColumn(col_qty, psf.when(psf.expr(filter_low_str),
                                                       sparkdf['{}_replace_low'.format(dict_key)]).otherwise(
            sparkdf[col_qty]))

    """落表"""
    return sparkdf.filter(date_filter_condition(sdate, edate))


def adjust_by_sales_days(df, edate, col_qty,col_key, col_time, w=2, agg_func='mean'):
    """前后7天无异常销量均值填补 w:周数"""
    c_columns = df.columns
    df = sales_continue(df, edate, col_qty, col_time,col_key)
    df['corr_qty'] = df[col_qty].rolling(window=15, center=True, min_periods=0).agg(agg_func)
    df['dayofweek'] = df.dt.apply(lambda x: x.weekday())
    df['corr_weekend_qty'] = df[df['dayofweek'].isin([5, 6])][col_qty].rolling(window=w * 2 + 1, center=True,
                                                                               min_periods=0).agg(agg_func)
    df['corr_weekdays_qty'] = df[~df['dayofweek'].isin([5, 6])][col_qty].rolling(window=w * 5 + 1, center=True,
                                                                                 min_periods=0).agg(agg_func)
    df['corr_dayofweek'] = df[['corr_weekend_qty', 'corr_weekdays_qty']].apply(lambda x: np.nanmax([x[1], x[0]]),
                                                                               axis=1)
    df[col_qty] = df[[col_qty, 'corr_dayofweek', 'corr_qty']].apply(
        lambda x: x[0] if not pd.isna(x[0]) else x[1] if not pd.isna(x[1]) else x[2], axis=1)
    return df[c_columns]


def adjust_by_sales_growth(df, col_key, col_qty, c_category,col_time, w=2, agg_func='mean'):
    """根据去年同期增长系数 同期范围定义 阴历 阳历 节假日 系数的倍数最大值最小值限制"""
    c_columns = df.columns
    df['last_dt'] = df.dt.apply(lambda x: x.replace(year=x.year - 1))
    col_key_last = col_key + [col_qty, 'dt']
    last_df = df[col_key_last].rename(columns={col_qty: 'last_qty', 'dt': 'last_dt'})
    df = pd.merge(df, last_df, left_on=col_key + ['last_dt'], right_on=col_key + ['last_dt'], how='left')

    df['last_qty_mean'] = df.groupby(col_key).last_qty.apply(
        lambda x: x.rolling(window=w * 7 + 1, center=True, min_periods=0).agg(agg_func))

    category_df = df.groupby(['dt', c_category])[col_qty].sum().reset_index()
    category_df['corr_qty'] = category_df[col_qty].rolling(window=w * 7 + 1, center=True, min_periods=0).agg(agg_func)

    category_last_df = df.groupby(['last_dt', c_category])['last_qty'].sum().reset_index()
    category_last_df['corr_last_qty'] = category_last_df['last_qty'].rolling(window=w * 7 + 1, center=True,
                                                                             min_periods=0).agg(agg_func)
    df = pd.merge(df, category_df[[c_category, 'dt', 'corr_qty']], on=[c_category, 'dt'], how='left')
    df = pd.merge(df, category_last_df[[c_category, 'last_dt', 'corr_last_qty']], on=[c_category, 'last_dt'],
                  how='left')
    print(df.head(10))
    df['ratio'] = df[['corr_qty', 'corr_last_qty']].apply(lambda x: compute_year_on_year_ratio(x[0], x[1]), axis=1)
    df[col_qty] = df[[col_qty, 'last_qty_mean', 'ratio']].apply(lambda x: x[0] if not pd.isna(x[0]) else x[1] * x[2],
                                                                axis=1)
    df[col_qty].fillna(0, inplace=True)
    df[col_time] = df[col_time].apply(lambda x:x.strftime('%Y%m%d'))
    
    return df[c_columns] 


def key_process(x, key):
    return tuple([x[key_c] for key_c in key])


def sales_day_abnormal_by_day_stock(openinv_value, endinv_value, qty_values):
    """期初或者期末一个为0认为缺货"""
    if (openinv_value is not None and endinv_value is not None) and (np.float64(openinv_value) <= 0.0 or np.float64(endinv_value) <= 0.0):
        return None
    else:
        return qty_values
    
def sales_check(ac_y_value,th_y_value):
    if (ac_y_value is not None and th_y_value is not None) and th_y_value < ac_y_value:
        return ac_y_value
    else:
        return th_y_value

def adjust_by_common(rows, key, edate, col_qty, col_key, c_category, col_time, w):
    """通用填充"""
    df = rdd_format_pdf(rows)
    df_step1 = adjust_by_sales_days(df, edate, col_qty, col_key, col_time, w)
    result_df = adjust_by_sales_growth(df_step1,col_key, col_qty, c_category,col_time,w)
    return pdf_format_rdd(result_df)


def no_sales_adjust(spark, param):
    """天级汇总销量、天级库存进行识别（无销售还原） sparkdf = sales+inv+category
       1.有期初、期末库存
       2.有期初、无期末
       3.无期初、有期末
    """
    col_openinv = param['col_openinv']
    col_endinv = param['col_endinv']
    col_key = param['col_key']
    join_key = param['join_key']
    col_category = param['col_category']
    col_qty = param['col_qty']
    edate = param['edate']
    sdate = param['sdate']
    col_time = param['col_time']
    w = param['w']
    input_sales_table = param['fill_zero_table']
    output_table = param['no_sales_adjust_table']
    input_stock_table = param['input_stock_table']
    input_category_table=param['input_category_table']
    select_list =join_key + [col_qty]
    
    sales_df = read_table(spark, input_sales_table)
    stock_df = read_origin_stock_table(spark, input_stock_table)
    categorty_df = read_origin_category_table(spark,input_category_table)
    sparkdf = sales_df.select(select_list).join(stock_df,on=join_key,how='left')
    sparkdf = sparkdf.join(categorty_df,on=col_key,how='left')
    sales_day_abnormal_by_day_stock_udf = udf(sales_day_abnormal_by_day_stock, DoubleType())
    sparkdf = sparkdf.withColumn("col_qty_tmp",
                                 sales_day_abnormal_by_day_stock_udf(sparkdf[col_openinv], sparkdf[col_endinv],
                                                                     sparkdf[col_qty]))
    sparkdf = sparkdf.rdd.map(lambda x: (key_process(x, col_key), x)).groupByKey().flatMap(
    lambda x: adjust_by_common(x[1], x[0], edate, "col_qty_tmp", col_key, col_category, col_time,w)).toDF()
    sparkdf = sparkdf.filter(date_filter_condition(sdate, edate))
    sparkdf = sparkdf.withColumnRenamed("col_qty_tmp", "th_y")
    sparkdf = sparkdf.withColumnRenamed(col_qty, "ac_y")
    sales_check_udf = udf(sales_check, DoubleType())
    sparkdf = sparkdf.withColumn("th_y",sales_check_udf(sparkdf['ac_y'],sparkdf['th_y']))
    save_table(spark, sparkdf, output_table)
    return 'SUCCESS'


def sales_abnormal_recognition_by_hour(qty_value, hour_value, out_stock_time_value):
    """异常销量打标"""
    print(qty_value, hour_value, out_stock_time_value)
    if not isinstance(out_stock_time_value, list):
        out_stock_time_value = []
    if not pd.isna(hour_value) and hour_value in out_stock_time_value:
        return np.nan
    else:
        return qty_value


def sales_reduction_by_hour(out_stock_min_value, actual_qty_value, ratio_value):
    """销量还原"""
    if pd.isna(out_stock_min_value):
        return actual_qty_value
    elif out_stock_min_value > 0:
        return actual_qty_value / ratio_value
    else:
        return actual_qty_value


def compute_ratio_by_category_hour(category_hour_value, category_sum_value):
    """计算小时级占比数据"""
    if not pd.isna(category_hour_value) and category_hour_value > 0:
        return category_hour_value / category_sum_value
    else:
        return np.nan


def sales_reduction_limit(actual_qty_value, sigma3_qty_value, rec_qty_value):
    return min(max(actual_qty_value, sigma3_qty_value), rec_qty_value)


def sales_fill_by_hour_inventory(sparkdf, col_key, col_qty, col_time, col_category, col_hour, col_out_stock_time, w):
    """小时级库存、小时级销量数据（缺货还原）"""
    #     sparkdf =  sales_sparkdf.join(stock_sparkdf,on=join_key,how='left')
    #     sparkdf = sparkdf.fillna('[]', subset=['out_stock_time'])
    windowOpt_hour = Window.partitionBy([col_category, col_hour, col_time]).orderBy(psf.col(col_time)).rangeBetween(
        start=-days(w), end=Window.currentRow)
    windowOpt_sum = Window.partitionBy([col_category, col_time]).orderBy(psf.col(col_time)).rangeBetween(start=-days(w),
                                                                                                         end=Window.currentRow)
    sparkdf = sparkdf.withColumn("category_hour", psf.sum(psf.col(col_qty)).over(windowOpt_hour))
    sparkdf = sparkdf.withColumn("category_sum", psf.sum(psf.col(col_qty)).over(windowOpt_sum))
    # 计算时段品类销售占比
    compute_ratio_by_category_hour_udf = udf(compute_ratio_by_category_hour, DoubleType())
    sparkdf = sparkdf.withColumn("ratio",
                                 compute_ratio_by_category_hour_udf(sparkdf.category_hour, sparkdf.category_sum))
    # 销量异常识别赋予空、时段占比赋予空
    sales_abnormal_recognition_by_hour_udf = udf(sales_abnormal_recognition_by_hour, DoubleType())
    sparkdf = sparkdf.withColumn("rec_qty", sales_abnormal_recognition_by_hour_udf(sparkdf[col_qty], sparkdf[col_hour],
                                                                                   sparkdf[col_out_stock_time]))
    sparkdf = sparkdf.withColumn("rec_ratio",
                                 sales_abnormal_recognition_by_hour_udf(sparkdf['ratio'], sparkdf[col_hour],
                                                                        sparkdf[col_out_stock_time]))
    # 销量还原-天级

    sparkdf_agg = sparkdf.groupBy(col_key.append(col_time)).agg(psf.sum(psf.col(col_qty)).alias("actual_qty"),
                                                                psf.sum(psf.col('rec_qty')).alias("rec_qty"),
                                                                psf.sum(psf.col('rec_ratio')).alias("ratio"),
                                                                psf.max(psf.col('out_stock_min')).alias(
                                                                    "out_stock_min"))

    sales_reduction_by_hour_udf = udf(sales_reduction_by_hour, DoubleType())
    sparkdf_agg = sparkdf_agg.withColumn("rec_qty", sales_reduction_by_hour_udf(sparkdf_agg['out_stock_min'],
                                                                                sparkdf_agg['rec_qty'],
                                                                                sparkdf_agg['ratio']))
    windowOpt_day = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w),
                                                                                      end=Window.currentRow)
    sparkdf_agg = sparkdf_agg.withColumn("qty_actual2", 2 * psf.col('actual_qty'))
    sparkdf_agg = sparkdf_agg.withColumn("qty_sigma3",
                                         psf.sum(psf.col('actual_qty')).over(windowOpt_day) + 3 * psf.stddev_pop(
                                             psf.col('actual_qty')).over(windowOpt_day))
    sales_reduction_limit_udf = udf(sales_reduction_limit, DoubleType())
    sparkdf_agg = sparkdf_agg.withColumn(col_qty, sales_reduction_limit_udf(sparkdf_agg['qty_actual2'],
                                                                            sparkdf_agg['qty_sigma3'],
                                                                            sparkdf_agg['rec_qty']))

    return sparkdf_agg


#     return sparkdf

def out_of_stock_adjust(spark, param):
    """***小时级缺货还原
       sales_sparkdf:销量数据 sparkdf=sales+inv+category
       inv_sparkdf:库存数据
       col_key:主键
       join_key:表关联键
       col_qty:销量字段
       col_hour:小时字段
       col_category:品类字段
       col_out_stock_time:缺货时段
       col_partition:分区字段
    """
    spark = param['spark']
    col_key = param['col_key']
    col_qty = param['col_qty']
    col_hour = param['col_hour']
    col_category = param['col_category']
    col_out_stock_time = param['col_out_stock_time']
    col_partition = param['col_partition']
    w = param['w']
    sdate = param['sdate']
    edate = param['edate']
    input_table = param['input_table']
    output_table = param['output_table']
    sparkdf = read_table(spark, input_table)
    sales_abnormal_recognition_by_hour_udf = udf(sales_abnormal_recognition_by_hour, DoubleType())
    # 异常识别
    sparkdf = sparkdf.withColumn(col_qty, sales_abnormal_recognition_by_hour_udf(sparkdf[col_qty],sparkdf[col_hour],
                                                                                         sparkdf[col_out_stock_time]))
    # 小时级缺货还原
    sparkdf = sales_fill_by_hour_inventory(sparkdf, col_key, col_qty, col_partition, col_category, col_hour,
                                           col_out_stock_time, w)
    sparkdf = sparkdf.filter(date_filter_condition(sdate, edate))
    save_table(sparkdf, output_table)
    return sparkdf

# sparkdf = spark.sql(
#     """select shop_id,goods_id,qty,dt,to_unix_timestamp(cast(dt as string),'yyyyMMdd') sdt from ai_dm.poc_feat_y""")
# sparkdf = boxcox_tranform(sparkdf, 'qty', 'qty_boxcox', "20190901", "20191231")
# sparkdf = adjust_by_bound(sparkdf, ['shop_id', 'goods_id'], "qty_boxcox", "qty", {'bound_sigma': (3, -3)}, "20190901",
#                           "20191231", {'bound_mean': (1, 1)}, 90, 'and')
# sparkdf.filter("qty>0 ").show(100)

def sales_fill_zero(spark,param):
    """标签过滤
          col_key：主键
          filter_func:过滤函数
          sdate:开始时间 '20210101'
          edate:结束时间
          col_qty:过滤字段
          w:时间窗口
          conn:函数之间关系
          col_time:时间戳字段
       """
    join_key = param['join_key']
    col_openinv = param['col_openinv']
    col_qty = param['col_qty']
    fill_value = param['fill_value']
    sdate = param['sdate']
    edate = param['edate']
    input_sales_table = param['qty_aggregation_table']
    input_stock_table = param['input_stock_table']
    output_table = param['fill_zero_table']
    sales_sparkdf = read_table(spark, input_sales_table)
    stock_sparkdf = read_origin_stock_table(spark, input_stock_table)
    sparkdf = adjust_by_column(sales_sparkdf, stock_sparkdf, join_key, col_openinv, col_qty, sdate, edate, fill_value)
    save_table(spark,sparkdf, output_table)
    return "SUCCESS"