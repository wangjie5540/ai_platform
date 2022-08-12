import datetime

from digitforce.aip.common.spark_helper import build_spark_session


def construct_sample(cur_str, output_file, train_duration=90, predict_duration=30,
                     label_column='deal_num', db_name='default'):
    spark = build_spark_session('sample_construct')
    cur_str_date = datetime.datetime.strptime(cur_str, '%Y%m%d')

    # 训练数据起始时间
    train_start_date_str = (cur_str_date - datetime.timedelta(days=train_duration)).strftime('%Y%m%d')
    # 预测起始时间
    predict_start_date = cur_str_date + datetime.timedelta(days=1)
    predict_start_date_str = predict_start_date.strftime('%Y%m%d')
    # 预测终止时间
    predict_end_date_str = (predict_start_date + datetime.timedelta(days=predict_duration)).strftime('%Y%m%d')

    # 近三天
    day3_str = (cur_str_date - datetime.timedelta(days=2)).strftime('%Y%m%d')
    # 近7天
    day7_str = (cur_str_date - datetime.timedelta(days=6)).strftime('%Y%m%d')
    # 近30天
    day30_str = (cur_str_date - datetime.timedelta(days=29)).strftime('%Y%m%d')
    # 近60天
    day60_str = (cur_str_date - datetime.timedelta(days=59)).strftime('%Y%m%d')
    # 近90天
    day90_str = (cur_str_date - datetime.timedelta(days=89)).strftime('%Y%m%d')
    # 近一年
    day365_str = (cur_str_date - datetime.timedelta(days=364)).strftime('%Y%m%d')

    label_data_sql = f"""
        select customerId
        ,case when b.customerId is not null then 1 else 0 end as label
        from
        (select distinct customerId from {db_name+'.'}deal 
        where dt between '{train_start_date_str}' and '{cur_str}' and {label_column} > 0)a
        left join
        (select distinct customerId from {db_name+'.'}deal
        where dt between '{predict_start_date_str}' and '{predict_end_date_str}' and {label_column} > 0)b
        on a.customerId = b.customerId
    """

    label_df = spark.sql(label_data_sql)
    label_df.createOrReplaceTempView('tmp_label')

    deal_stat_sql = f"""
        select customerId
        ,sum(case when dt >= '{day3_str}' then dealNum end) as day3_dealNum
        ,sum(case when dt >= '{day3_str}' then abs(dealSum) end) as day3_dealSum
        ,sum(case when dt >= '{day3_str}' and dealSum < 0 then abs(dealSum) end) as day3_out_dealSum
        ,sum(case when dt >= '{day3_str}' then dealStockNum end) as day3_dealStockNum
        ,sum(case when dt >= '{day3_str}' then dealStockSum end) as day3_dealStockSum
        ,sum(case when dt >= '{day3_str}' then dealFundNum end) as day3_dealFundNum
        ,sum(case when dt >= '{day3_str}' then dealFundSum end) as day3_dealFundSum
        ,sum(case when dt >= '{day3_str}' then closeNum end) as day3_closeNum
        ,count(case when dt >= '{day3_str}' then dealNum end) as day3_deal_active_num
        ,sum(case when dt >= '{day7_str}' then dealNum end) as day7_dealNum
        ,sum(case when dt >= '{day7_str}' then abs(dealSum) end) as day7_dealSum
        ,sum(case when dt >= '{day7_str}' and dealSum < 0 then abs(dealSum) end) as day7_out_dealSum
        ,sum(case when dt >= '{day7_str}' then dealStockNum end) as day7_dealStockNum
        ,sum(case when dt >= '{day7_str}' then dealStockSum end) as day7_dealStockSum
        ,sum(case when dt >= '{day7_str}' then dealFundNum end) as day7_dealFundNum
        ,sum(case when dt >= '{day7_str}' then dealFundSum end) as day7_dealFundSum
        ,sum(case when dt >= '{day7_str}' then closeNum end) as day7_closeNum
        ,count(case when dt >= '{day7_str}' then dealNum end) as day7_deal_active_num
        ,sum(case when dt >= '{day30_str}' then dealNum end) as day30_dealNum
        ,sum(case when dt >= '{day30_str}' then abs(dealSum) end) as day30_dealSum
        ,sum(case when dt >= '{day30_str}' and dealSum < 0 then abs(dealSum) end) as day30_out_dealSum
        ,sum(case when dt >= '{day30_str}' then dealStockNum end) as day30_dealStockNum
        ,sum(case when dt >= '{day30_str}' then dealStockSum end) as day30_dealStockSum
        ,sum(case when dt >= '{day30_str}' then dealFundNum end) as day30_dealFundNum
        ,sum(case when dt >= '{day30_str}' then dealFundSum end) as day30_dealFundSum
        ,sum(case when dt >= '{day30_str}' then closeNum end) as day30_closeNum
        ,count(case when dt >= '{day30_str}' then dealNum end) as day30_deal_active_num
        ,sum(case when dt >= '{day60_str}' then dealNum end) as day60_dealNum
        ,sum(case when dt >= '{day60_str}' then abs(dealSum) end) as day60_dealSum
        ,sum(case when dt >= '{day3_str}' and dealSum < 0 then abs(dealSum) end) as day60_out_dealSum
        ,sum(case when dt >= '{day60_str}' then dealStockNum end) as day60_dealStockNum
        ,sum(case when dt >= '{day60_str}' then dealStockSum end) as day60_dealStockSum
        ,sum(case when dt >= '{day60_str}' then dealFundNum end) as day60_dealFundNum
        ,sum(case when dt >= '{day60_str}' then dealFundSum end) as day60_dealFundSum
        ,sum(case when dt >= '{day60_str}' then closeNum end) as day60_closeNum
        ,count(case when dt >= '{day60_str}' then dealNum end) as day60_deal_active_num
        ,sum(case when dt >= '{day90_str}' then dealNum end) as day90_dealNum
        ,sum(case when dt >= '{day90_str}' then abs(dealSum) end) as day90_dealSum
        ,sum(case when dt >= '{day3_str}' and dealSum < 0 then abs(dealSum) end) as day90_out_dealSum
        ,sum(case when dt >= '{day90_str}' then dealStockNum end) as day90_dealStockNum
        ,sum(case when dt >= '{day90_str}' then dealStockSum end) as day90_dealStockSum
        ,sum(case when dt >= '{day90_str}' then dealFundNum end) as day90_dealFundNum
        ,sum(case when dt >= '{day90_str}' then dealFundSum end) as day90_dealFundSum
        ,sum(case when dt >= '{day90_str}' then closeNum end) as day90_closeNum
        ,count(case when dt >= '{day90_str}' then dealNum end) as day90_deal_active_num
        ,sum(case when dt >= '{day365_str}' then dealNum end) as day365_dealNum
        ,sum(case when dt >= '{day365_str}' then abs(dealSum) end) as day365_dealSum
        ,sum(case when dt >= '{day3_str}' and dealSum < 0 then abs(dealSum) end) as day365_out_dealSum
        ,sum(case when dt >= '{day365_str}' then dealStockNum end) as day365_dealStockNum
        ,sum(case when dt >= '{day365_str}' then dealStockSum end) as day365_dealStockSum
        ,sum(case when dt >= '{day365_str}' then dealFundNum end) as day365_dealFundNum
        ,sum(case when dt >= '{day365_str}' then dealFundSum end) as day365_dealFundSum
        ,sum(case when dt >= '{day365_str}' then closeNum end) as day365_closeNum
        ,count(case when dt >= '{day365_str}' then dealNum end) as day365_deal_active_num
        ,min(datediff(from_unixtime(UNIX_TIMESTAMP('{cur_str}', 'yyyymmdd')), 
                      from_unixtime(UNIX_TIMESTAMP(dt, 'yyyymmdd')))) as deal_active_duration_days
        from {db_name+'.'}deal
        where dt between '{day365_str}' and '{cur_str}' and {label_column} > 0
        group by customerId
    """
    deal_stat_df = spark.sql(deal_stat_sql)
    deal_stat_df.createOrReplaceTempView('tmp_deal_stat')

    app_stat_sql = f"""
        select customerId 
        ,sum(case when dt >= {day3_str} then isLogin end) as day3_login_num
        ,sum(case when dt >= {day3_str} then newsClick end) as day3_news_click_num
        ,sum(case when dt >= {day7_str} then isLogin end) as day7_login_num
        ,sum(case when dt >= {day7_str} then newsClick end) as day7_news_click_num
        ,sum(case when dt >= {day30_str} then isLogin end) as day30_login_num
        ,sum(case when dt >= {day30_str} then newsClick end) as day30_news_click_num
        ,sum(case when dt >= {day60_str} then isLogin end) as day60_login_num
        ,sum(case when dt >= {day60_str} then newsClick end) as day60_news_click_num
        ,sum(case when dt >= {day90_str} then isLogin end) as day90_login_num
        ,sum(case when dt >= {day90_str} then newsClick end) as day90_news_click_num
        ,sum(case when dt >= {day365_str} then isLogin end) as day365_login_num
        ,sum(case when dt >= {day365_str} then newsClick end) as day365_news_click_num
        ,min(datediff(from_unixtime(UNIX_TIMESTAMP('{cur_str}', 'yyyymmdd')), 
                      from_unixtime(UNIX_TIMESTAMP(dt, 'yyyymmdd')))) as app_active_duration_days
        from {db_name+'.'}app
        where dt between '{day365_str}' and '{cur_str}' and {label_column} > 0 and isLogin=1
        group by customerId
    """
    app_stat_df = spark.sql(app_stat_sql)
    app_stat_df.createOrReplaceTempView('tmp_app_stat')

    last_stat_sql = f"""
        select * from(
        select customerId, assetPonit, sebtPoint, remainingPoint, fundPoint, fundNumPoint, stockPoint, stockNumPoint, 
        financePoint, shortsalePoint, assetOutPoint, dt, row_number()over(partition by customerId order by dt desc) rk
        from {db_name+'.'}account
        where dt between '{train_start_date_str}' and '{cur_str}') a 
        where rk = 1
    """
    last_stat_df = spark.sql(last_stat_sql)
    last_stat_df.createOrReplaceTempView('tmp_last_stat')

    example_sql = f"""
        select t1.customerId
            ,t1.label
            ,t2.sex,
            ,t2.age
            ,t2.riskScore1
            ,t2.riskScore2
            ,datediff(from_unixtime(unix_timestamp('{cur_str}', 'yyyymmdd')),
                      from_unixtime(unix_timestamp(t2.dateOpen, 'yyyymmdd'))) as open_duration_days
            ,t2.customerType
            ,coalesce(t3.day3_dealNum, 0) as day3_dealNum
            ,coalesce(t3.day3_dealSum, 0) as day3_dealSum
            ,coalesce(t3.day3_out_dealSum, 0) as day3_out_dealSum
            ,coalesce(t3.day3_dealStockNum, 0) as day3_dealStockNum
            ,coalesce(t3.day3_dealStockSum, 0) as day3_dealStockSum
            ,coalesce(t3.day3_dealFundNum, 0) as day3_dealFundNum
            ,coalesce(t3.day3_dealFundSum, 0) as day3_dealFundSum
            ,coalesce(t3.day3_closeNum, 0) as day3_closeNum
            ,coalesce(t3.day3_deal_active_num, 0) as day3_deal_active_num
            ,coalesce(t3.day7_dealNum, 0) as day7_dealNum
            ,coalesce(t3.day7_dealSum, 0) as day7_dealSum
            ,coalesce(t3.day7_out_dealSum, 0) as day7_out_dealSum
            ,coalesce(t3.day7_dealStockNum, 0) as day7_dealStockNum
            ,coalesce(t3.day7_dealStockSum, 0) as day7_dealStockSum
            ,coalesce(t3.day7_dealFundNum, 0) as day7_dealFundNum
            ,coalesce(t3.day7_dealFundSum, 0) as day7_dealFundSum
            ,coalesce(t3.day7_closeNum, 0) as day7_closeNum
            ,coalesce(t3.day7_deal_active_num, 0) as day7_deal_active_num
            ,coalesce(t3.day30_dealNum, 0) as day30_dealNum
            ,coalesce(t3.day30_dealSum, 0) as day30_dealSum
            ,coalesce(t3.day30_out_dealSum, 0) as day30_out_dealSum
            ,coalesce(t3.day30_dealStockNum, 0) as day30_dealStockNum
            ,coalesce(t3.day30_dealStockSum, 0) as day30_dealStockSum
            ,coalesce(t3.day30_dealFundNum, 0) as day30_dealFundNum
            ,coalesce(t3.day30_dealFundSum, 0) as day30_dealFundSum
            ,coalesce(t3.day30_closeNum, 0) as day30_closeNum
            ,coalesce(t3.day30_deal_active_num, 0) as day30_deal_active_num
            ,coalesce(t3.day60_dealNum, 0) as day60_dealNum
            ,coalesce(t3.day60_dealSum, 0) as day60_dealSum
            ,coalesce(t3.day60_out_dealSum, 0) as day60_out_dealSum
            ,coalesce(t3.day60_dealStockNum, 0) as day60_dealStockNum
            ,coalesce(t3.day60_dealStockSum, 0) as day60_dealStockSum
            ,coalesce(t3.day60_dealFundNum, 0) as day60_dealFundNum
            ,coalesce(t3.day60_dealFundSum, 0) as day60_dealFundSum
            ,coalesce(t3.day60_closeNum, 0) as day60_closeNum
            ,coalesce(t3.day60_deal_active_num, 0) as day60_deal_active_num
            ,coalesce(t3.day90_dealNum, 0) as day90_dealNum
            ,coalesce(t3.day90_dealSum, 0) as day90_dealSum
            ,coalesce(t3.day90_out_dealSum, 0) as day90_out_dealSum
            ,coalesce(t3.day90_dealStockNum, 0) as day90_dealStockNum
            ,coalesce(t3.day90_dealStockSum, 0) as day90_dealStockSum
            ,coalesce(t3.day90_dealFundNum, 0) as day90_dealFundNum
            ,coalesce(t3.day90_dealFundSum, 0) as day90_dealFundSum
            ,coalesce(t3.day90_closeNum, 0) as day90_closeNum
            ,coalesce(t3.day90_deal_active_num, 0) as day90_deal_active_num
            ,coalesce(t3.day365_dealNum, 0) as day365_dealNum
            ,coalesce(t3.day365_dealSum, 0) as day365_dealSum
            ,coalesce(t3.day365_out_dealSum, 0) as day365_out_dealSum
            ,coalesce(t3.day365_dealStockNum, 0) as day365_dealStockNum
            ,coalesce(t3.day365_dealStockSum, 0) as day365_dealStockSum
            ,coalesce(t3.day365_dealFundNum, 0) as day365_dealFundNum
            ,coalesce(t3.day365_dealFundSum, 0) as day365_dealFundSum
            ,coalesce(t3.day365_closeNum, 0) as day365_closeNum
            ,coalesce(t3.day365_deal_active_num, 0) as day365_deal_active_num
            ,t3.deal_active_duration_days
            ,coalesce(t4.day3_login_num, 0) as day3_login_num
            ,coalesce(t4.day3_news_click_num, 0) as day3_news_click_num
            ,coalesce(t4.day7_login_num, 0) as day7_login_num
            ,coalesce(t4.day7_news_click_num, 0) as day7_news_click_num
            ,coalesce(t4.day30_login_num, 0) as day30_login_num
            ,coalesce(t4.day30_news_click_num, 0) as day30_news_click_num
            ,coalesce(t4.day60_login_num, 0) as day60_login_num
            ,coalesce(t4.day60_news_click_num, 0) as day60_news_click_num
            ,coalesce(t4.day90_login_num, 0) as day90_login_num
            ,coalesce(t4.day90_news_click_num, 0) as day90_news_click_num
            ,coalesce(t4.day365_login_num, 0) as day365_login_num
            ,coalesce(t4.day365_news_click_num, 0) as day365_news_click_num
            ,t4.app_active_duration_days
            ,t5.assetPonit as last_assetPonit
            ,t5.sebtPoint as last_sebtPoint
            ,t5.remainingPoint as last_remainingPoint
            ,t5.fundPoint as last_fundPoint
            ,t5.fundNumPoint as last_fundNumPoint
            ,t5.stockPoint as last_stockPoint
            ,t5.stockNumPoint as last_stockNumPoint
            ,t5.financePoint as last_financePoint
            ,t5.shortsalePoint as last_shortsalePoint
            ,t5.assetOutPoint as last_assetOutPoint
        from tmp_label t1
        left join {db_name+'.'}customer t2 on t1.customerId = t2.customerId
        left join tmp_deal_stat t3 on t1.customerId = t3.customerId
        left join tmp_app_stat t4 on t1.customerId = t4.customerId
        left join tmp_last_stat t5 on t1.customerId = t5.customerId 
    """

    example = spark.sql(example_sql)
    example.write.mode('overwrite').option('header', 'true').format('csv').csv(f"{output_file}/{cur_str}")
