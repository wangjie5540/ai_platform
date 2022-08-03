# -*- coding: utf-8 -*-
# @Time : 2021/12/25
# @Author : Arvin
from digitforce.aip.common.data_helper import tuple_self
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


def get_item_config_info(spark, param):
    """
    获取保质期、销售期、是否标品、是否新品等配置信息
    """

    input_table = param['input_table']
    config_columns = param['config_columns']
    col_key = param['col_key']  # 商品分类所用的主字段
    shops = param['partition_list']
    output_table = param['item_config_table']
    output_partition_name = param['input_partition_name']
    sparkdf_sales = read_table(spark, input_table, partition_list=shops)  # 读取销售表

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
    save_table(spark, sparkdf_item.select(config_columns + col_key), output_table, partition=output_partition_name)
    return 'SUCCESS'