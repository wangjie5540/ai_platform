# -*- coding: utf-8 -*-
# @Time : 2022/06/29
# @Author : Arvin
from forecast.common.mysql import get_data_from_mysql
from forecast.common.reference_package import *
from digitforce.aip.common.spark_helper import *


def is_new_sku(standard, storage_life, sale_days):
    """
    storage_life:保质期
    standard:是否标品
    sale_days:历史销售天
    """
    if standard == 1 and storage_life > 180 and (sale_days is not None and sale_days) < 30:
        return 1
    elif sale_days is not None and sale_days < 17:
        return 1
    else:
        return 0


def get_sale_period(storage_life, days):
    """
    有销售期返回销售期，否则返回 1/3 保质期
    """
    if days is not None and days > 0:
        return int(days)
    elif storage_life > 0:
        return int(storage_life / 3)
    else:
        return 1


def get_item_config_info(spark, sparkdf_sales, config_columns, col_key, shops):
    """
    获取保质期、销售期、是否标品、是否新品等配置信息
    """
    #     query_sql = """select * from {0}""".format(item_config_table)
    query_sql = """
    select t1.item_code as goods_id,t2.site_code as shop_id,t1.subsidiary
     ,cast(case when t1.sale_days is null then -1 else t1.sale_days end as SIGNED) sale_period
     ,cast(case when t1.strategy ='FreshProductNoStandard' and (t1.storage_life<=0 or t1.storage_life is null) then 3 
		   when (t1.strategy <> 'FreshProductNoStandard' OR t1.strategy IS NULL) and 
		        (t1.storage_life<=0 or t1.storage_life is null) then 90
		   else t1.storage_life  end as SIGNED) storage_life
     ,cast(t1.standard as CHAR) standard
     ,case when t2.loss_rate is null then 0.0 else t2.loss_rate end loss_rate
     ,case when t2.sys_loss_rate is null then 0.0 else t2.sys_loss_rate end sys_loss_rate
     from (
     select * from (
     select 
     @num := if(@temp_item =item_code and @temp_subsidiary=subsidiary,@num:= @num+1,1) as rank
    ,@temp_item := item_code as item_code
    ,@temp_subsidiary := subsidiary as subsidiary
    ,sale_days
    ,storage_life
    ,standard
    ,on_sales
    ,create_time
    ,strategy 
    from item where deleted=0
    ) t where rank=1) t1
    join
    (select site_code,subsidiary,item_code,loss_rate,sys_loss_rate from site_item where deleted=0 and site_code in {0}) t2
    on t1.item_code = t2.item_code and t1.subsidiary = t2.subsidiary
    """.format(tuple_self(shops))
    df_item = get_data_from_mysql(query_sql)
    sparkdf_item = spark.createDataFrame(df_item)
    sparkdf_days = sparkdf_sales.groupby(col_key).agg(psf.count(psf.col(col_key[0])).alias("sale_days"))  # 计算销售天数
    sparkdf_item = sparkdf_item.join(sparkdf_days, on=col_key, how='left')
    is_new_sku_udf = udf(is_new_sku, StringType())
    sparkdf_item = sparkdf_item.withColumn("is_new",
                                           is_new_sku_udf(sparkdf_item['standard'], sparkdf_item['storage_life'],
                                                          sparkdf_item['sale_days']))
    get_sale_period_udf = udf(get_sale_period, IntegerType())
    sparkdf_item = sparkdf_item.withColumn("sale_period", get_sale_period_udf(sparkdf_item['storage_life'],
                                                                              sparkdf_item['sale_period']))
    return sparkdf_item.select(config_columns + col_key)





def data_prepare(spark, params_data_prepare):
    method = params_data_prepare['method']
    sales_table = params_data_prepare['sales_table']
    col_key = params_data_prepare['col_key']
    config_columns = params_data_prepare['config_columns']
    col_sku_category = params_data_prepare['col_sku_category']
    shops = params_data_prepare['shop_list']
    w_tail = params_data_prepare['w_tail']
    w_ls = params_data_prepare['w_ls']
    col_time = params_data_prepare['col_time']
    col_qty = params_data_prepare['col_qty']
    threshold_count = params_data_prepare['threshold_count']
    edate = params_data_prepare['edate']
    threshold_proportion = params_data_prepare['threshold_proportion']
    threshold_sales = params_data_prepare['threshold_sales']
    # data_prepare_table = params_data_prepare['data_prepare_table']
    sparkdf_sales = forecast_spark_helper.read_table(sales_table, sdt='N', partition_list=shops)
    config_tables = params_data_prepare['config_tables']
    config_table_list = []

    # 销量分层
    # if 'sales_classfify' in method:
    #     sparkdf_classfiy = read_table()
    #     config_table_list.append(sparkdf_classfiy)
    #
    # # 配置信息
    # if 'item_config' in method:
    #     sparkdf_item = get_item_config_info(spark, sparkdf_sales, config_columns, col_key, shops)
    #     config_table_list.append(sparkdf_item)
    #
    # if '' in method:
    #     pass

    table_names = locals()
    for table_name in config_tables:
        table_names[table_name] = forecast_spark_helper.read_table(table_name)
        config_table_list.append(table_names[table_name])

    merge_info = reduce(lambda l, r: l.join(r, col_key, 'left'), config_table_list)
    merge_info.select(config_columns + [col_sku_category])
    # save_table(spark, merge_info, data_prepare_table, partition=["shop_id"])
    print("数据准备已经完成！")
    return merge_info
