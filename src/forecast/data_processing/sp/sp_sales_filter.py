# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin
# 基于spark版
from forecast.common.common_helper import *
oldtime = datetime.datetime.now()
# sparkdf = spark.sql("""
#           select *,to_unix_timestamp(cast(sdt as string),'yyyyMMdd') dt,
#           weekofyear(from_unixtime(unix_timestamp(cast(sdt as string),'yyyyMMdd'),'yyyy-MM-dd')) solar_week,
#           dayofweek(from_unixtime(unix_timestamp(cast(sdt as string),'yyyyMMdd'),'yyyy-MM-dd')) dayofweek,
#           month(from_unixtime(unix_timestamp(cast(sdt as string),'yyyyMMdd'),'yyyy-MM-dd')) month
#           from ai_dm.poc_data_test""")


def tran_boxcox_compute(x):
    """
    boxcox转化
    :param x: 要转化的值
    :return:
    """
    if x<0:
        x=0
    tmp = 0.01  # 如果为0，那么默认加个临时数
    x = x + 1  # 对x进行+1处理
    tmp2 = 0.01  # 这是在boxcox转化的时候默认使用临时变量
    if tmp2 == x:
        tmp2 = x + 1
    x_new = [x, tmp2]
    y = stats.boxcox(np.array(x_new))[0].tolist()[0]
    return y


def bound_sigma(windowOpt, col_value, paramter):
    """
    spark窗口补历史数据
    """
    up = paramter[0]
    low = paramter[1]
    print(up, low)
    std_up = up * psf.stddev_pop(psf.col(col_value)).over(windowOpt)
    std_low = low * psf.stddev_pop(psf.col(col_value)).over(windowOpt)
    avg = psf.avg(psf.col(col_value)).over(windowOpt)
    return avg + std_up, avg + std_low


def bound_percentile(windowOpt, col_value, paramter):
    up = paramter[0]
    low = paramter[1]
    percentile_up = psf.expr('percentile_approx({0}, {1})'.format(col_value, up)).over(windowOpt)
    percentile_low = psf.expr('percentile_approx({0}, {1})'.format(col_value, low)).over(windowOpt)
    return percentile_up, percentile_low


def bound_boxcox(sparkdf, col_qty, col_boxcox):
    """boxcox变换"""
    tran_boxcox = udf(tran_boxcox_compute, DoubleType())
    sparkdf = sparkdf.withColumn(col_boxcox, tran_boxcox(sparkdf[col_qty]))
    return sparkdf


def bound_iqr(windowOpt, col_value, paramter):
    up = paramter[0]
    low = paramter[1]
    percentile_up = psf.expr('percentile_approx({0}, {1})'.format(col_value, 0.75)).over(windowOpt)
    percentile_low = psf.expr('percentile_approx({0}, {1})'.format(col_value, 0.25)).over(windowOpt)
    iqr = percentile_up - percentile_low
    return up * iqr, low * iqr


def bound_mean(windowOpt, col_value, paramter):
    up = paramter[0]
    low = paramter[1]
    avg = psf.avg(psf.col(col_value)).over(windowOpt)
    return up * avg, low * avg


def bound_count(windowOpt, col_value, paramter):
    up = paramter[0]
    low = paramter[1]
    countnum = psf.count(psf.col(col_value)).over(windowOpt)
    return up * countnum, low * countnum


def bound_fix(windowOpt, col_value, paramter):
    up = paramter[0]
    low = paramter[1]
    return "{0}>={1} and {0}<={2}".format(col_value, low, up)


def boxcox_tranform(sparkdf, col_qty, col_boxcox, sdate, edate):
    """coxbox变换"""
    sparkdf = bound_boxcox(sparkdf, col_qty, col_boxcox)
    return sparkdf.filter(date_filter_condition(sdate, edate))


def adjust_by_bound(sparkdf, col_key, col_boxcox, col_qty, filter_func, sdate, edate, replace_func, w_boxcox=90, w_replace=14,conn='and',
                    col_time='sdt'):
    """
    过去一段时间窗口w内 ，通过filter_func超出上下限的用replace_func的边界值替换
    """
    windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w_boxcox),
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
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w_replace),
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


def sales_filter_by_boxcox(spark, param):
    col_key = param['col_key']
    col_qty = param['col_qty']
    w_boxcox = param['w_boxcox']
    w_replace = param['w_replace']
    sdate = param['sdate']
    edate = param['edate']
    func_dict_boxcox = eval(param['func_dict_boxcox'])
    replace_func_boxcox = eval(param['replace_func_boxcox'])
    conn = param['conn']
    col_time = param['col_time']
    input_table = param['no_sales_adjust_table']
    output_table = param['sales_boxcox_table']
    col_boxcox = param['col_boxcox']
    sparkdf = read_table(spark, input_table, sdt='N')
    sparkdf = boxcox_tranform(sparkdf, col_qty, col_boxcox, sdate, edate)

    sparkdf = adjust_by_bound(sparkdf, col_key, col_boxcox, col_qty, func_dict_boxcox, sdate, edate, replace_func_boxcox,
                          w_boxcox, w_replace, conn, col_time)
    save_table(spark, sparkdf, output_table,)
    return 'SUCCESS'



def sales_filter_by_bound(sparkdf, key, w, sdate, edate, col_qty, func_dict, conn='and', col_time='sdt'):
    """
    订单过滤by bound
    """
    windowOpt = Window.partitionBy(key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w-1), end=Window.currentRow)
    for dict_key in func_dict:
        print(dict_key)
        func_up, func_low = globals()[dict_key](windowOpt, col_qty, func_dict[dict_key])
        print(func_up)
        sparkdf = sparkdf.withColumn("{}_up".format(dict_key), func_up)
        sparkdf = sparkdf.withColumn("{}_low".format(dict_key), func_low)
    if conn == "and":
        filter_str = "1=1 "
        for dict_key in func_dict:
            filter_str += "and {0}<={1}_up ".format(col_qty, dict_key)
            filter_str += "and {0}>={1}_low ".format(col_qty, dict_key)
    else:
        for dict_key in func_dict:
            filter_str += "or {0}<={1}_up ".format(col_qty, dict_key)
            filter_str += "or {0}>={1}_low ".format(col_qty, dict_key)

    sparkdf = sparkdf.filter(filter_str)
    """落表"""
    return sparkdf.filter(date_filter_condition(sdate, edate))


def sales_filter_by_label(sparkdf, col_label, filter_value, sdate, edate):
    """
    filter_value:list4
    """
    if len(filter_value) == 1:
        filter_str = "{0} <> {1}".format(col_label, filter_value)
    elif len(filter_value) > 1:
        filter_str = "{0} not in {1}".format(col_label, tuple(filter_value))
    else:
        filter_str = ""

    sparkdf = sparkdf.filter(filter_str)
    return sparkdf.filter(date_filter_condition(sdate, edate))


def sales_fliter_by_label_price(sparkdf, col_key, col_label, filter_value, col_price, discount, price_func_dict, sdate,
                                edate, col_time, w=90, conn='and'):
    """出清过滤特殊字段标识或者通过价格过滤"""
    if col_label != "":
        sparkdf = sales_filter_by_label(sparkdf,col_label, filter_value, sdate, edate)
    else:
        windowOpt = Window.partitionBy(col_key).orderBy(psf.col(col_time)).rangeBetween(start=-days(w-1), end=Window.currentRow)
        for dict_key in price_func_dict:
            func_up, func_low = globals()[dict_key](windowOpt,col_price,price_func_dict[dict_key])
            sparkdf = sparkdf.withColumn("{}_up".format(dict_key), func_up)
            sparkdf = sparkdf.withColumn("{}_low".format(dict_key), func_low)
        if conn == "and":
            filter_str = "1=1 "
            for dict_key in price_func_dict:
                filter_str += "and {0}<={2}*{1}_up ".format(col_price,dict_key,discount)
                filter_str += "and {0}>={1}_low ".format(col_price,dict_key)
        else:
            for dict_key in price_func_dict:
                filter_str += "or {0}<={1}_up ".format(col_price,dict_key)
                filter_str += "or {0}>={1}_low ".format(col_price,dict_key)
        sparkdf = sparkdf.filter(filter_str)
    """落表"""
    return sparkdf.filter(date_filter_condition(sdate, edate))


def sales_clearance_filter(param):
    """订单过滤
       col_key：主键
       col_label:过滤字段
       filter_value:过滤字段值
       col_price:售价
       discount:折扣
       price_func_dict:价格过滤函数
       sdate:开始时间
       edate:结束时间
    """
    spark = param['spark']
    col_key = param['col_key']
    col_label = param['col_label']
    filter_value = param['filter_value']
    col_price = param['col_price']
    discount = param['discount']
    price_func_dict = eval(param['price_func_dict'])
    w = param['w']
    sdate = param['sdate']
    edate = param['edate']
    col_time = param['col_time']
    conn = param['conn']
    input_table = param['input_table']
    output_table = param['output_table']
    sparkdf = read_table(spark, input_table)

    sparkdf = sales_fliter_by_label_price(sparkdf, col_key, col_label, filter_value, col_price, discount,
                                          price_func_dict, sdate, edate, col_time, w, conn)
    save_table(sparkdf, output_table)
    return sparkdf


def big_order_filter(spark, param):
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
    w = param['w']
    sdate = param['sdate']
    edate = param['edate']
    col_time = param['col_time']
    col_qty = param['col_qty']
    filter_func = eval(param['filter_func'])
    conn = param['conn']
    input_table = param['input_sales_table']
    output_table = param['outlier_order_table']
    col_partitions = param['col_partitions']
    shop_list = param['shop_list']
    sparkdf = read_origin_sales_table(spark, input_table,shop_list=shop_list)
    sparkdf = sales_filter_by_bound(sparkdf, col_key, w, sdate, edate, col_qty, filter_func, conn)
    save_table(spark, sparkdf, output_table)
    return "SUCCESS"


def filter_by_bound(spark, param):
    """阈值过滤
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
    w = param['w']
    sdate = param['sdate']
    edate = param['edate']
    col_time = param['col_time']
    col_qty = param['col_qty']
    filter_func = eval(param['filter_func'])
    conn = param['conn']
    input_table = param['input_table']
    output_table = param['output_table']
    sparkdf = read_table(spark, input_table)
    sparkdf = sales_filter_by_bound(sparkdf, col_key, w, sdate, edate, col_time, col_qty, filter_func, conn)
    save_table(sparkdf, output_table)
    return 'SUCCESS'


def filter_by_label(param):
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
    spark = param['spark']
    col_label = param['col_label']
    filter_value = param['filter_value']
    sdate = param['sdate']
    edate = param['edate']
    input_table = param['input_table']
    output_table = param['output_table']
    sparkdf = read_table(spark, input_table)
    sparkdf = sales_filter_by_label(sparkdf, col_label, filter_value, sdate, edate)
    save_table(sparkdf, output_table)
    return sparkdf





# sales_filter(sparkdf, ['shop_id', 'goods_id'], {'bound_sigma': (3, -3), 'boud_percentile': (0.99, 0.00)}, "20210101",
#              "20210401", "sdt").show(10)
# sales_clear_filter(sparkdf,col_key,col_label,filter_value,col_price,0.5,{'boud_percentile':(0.75, 0.00)},sdate,edate,30,conn)


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
#     print("wwww", sparkdf.show(10))
    sales_fill_udf = udf(sales_fill, DoubleType())
    sparkdf = sparkdf.withColumn("fill_{}".format(col_qty),sales_fill_udf(sparkdf[col_qty], sparkdf[col_openinv], sparkdf.fill_tmp))
    print("dsdsddsdsd",sparkdf.show(10))
    return sparkdf.filter(date_filter_condition(sdate, edate))
                        

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
    print("wo zai sale filter")
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