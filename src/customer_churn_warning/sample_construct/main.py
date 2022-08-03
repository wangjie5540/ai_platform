import datetime
import os
import logging
import argparse

from digitforce.aip.common.hive_helper import df_hive_helper
from digitforce.aip.common.logging_config import setup_logging


def get_examples(cur_str, output_file, train_duration=90, predict_duration=30,
                 label_column='deal_num', db_name='default'):
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

    sql = f"""
    with tmp_label as (
        select customerId
        ,case when b.customerId is not null then 1 else 0 end as label
        from
        (select distinct customerId from {db_name+'.'}deal 
        where dt between '{train_start_date_str}' and '{cur_str}' and {label_column} > 0)a
        left join
        (select distinct customerId from {db_name+'.'}deal
        where dt between '{predict_start_date_str}' and '{predict_end_date_str}' and {label_column} > 0)b
        on a.customerId = b.customerId
    ),
    
    tmp_deal_stat as (
        select customerId
        ,sum(case when dt >= '{day3_str}' then dealNum end) as day3_dealNum
        ,sum(case when dt >= '{day3_str}' then dealSum end) as day3_dealSum
        ,sum(case when dt >= '{day3_str}' then dealStockNum end) as day3_dealStockNum
        ,sum(case when dt >= '{day3_str}' then dealStockSum end) as day3_dealStockSum
        ,sum(case when dt >= '{day3_str}' then dealFundNum end) as day3_dealFundNum
        ,sum(case when dt >= '{day3_str}' then dealFundSum end) as day3_dealFundSum
        ,sum(case when dt >= '{day3_str}' then closeNum end) as day3_closeNum
        ,count(case when dt >= '{day3_str}' then dealNum end) as day3_deal_active_num
        ,sum(case when dt >= '{day7_str}' then dealNum end) as day7_dealNum
        ,sum(case when dt >= '{day7_str}' then dealSum end) as day7_dealSum
        ,sum(case when dt >= '{day7_str}' then dealStockNum end) as day7_dealStockNum
        ,sum(case when dt >= '{day7_str}' then dealStockSum end) as day7_dealStockSum
        ,sum(case when dt >= '{day7_str}' then dealFundNum end) as day7_dealFundNum
        ,sum(case when dt >= '{day7_str}' then dealFundSum end) as day7_dealFundSum
        ,sum(case when dt >= '{day7_str}' then closeNum end) as day7_closeNum
        ,count(case when dt >= '{day7_str}' then dealNum end) as day7_deal_active_num
        ,sum(case when dt >= '{day30_str}' then dealNum end) as day30_dealNum
        ,sum(case when dt >= '{day30_str}' then dealSum end) as day30_dealSum
        ,sum(case when dt >= '{day30_str}' then dealStockNum end) as day30_dealStockNum
        ,sum(case when dt >= '{day30_str}' then dealStockSum end) as day30_dealStockSum
        ,sum(case when dt >= '{day30_str}' then dealFundNum end) as day30_dealFundNum
        ,sum(case when dt >= '{day30_str}' then dealFundSum end) as day30_dealFundSum
        ,sum(case when dt >= '{day30_str}' then closeNum end) as day30_closeNum
        ,count(case when dt >= '{day30_str}' then dealNum end) as day30_deal_active_num
        ,sum(case when dt >= '{day60_str}' then dealNum end) as day60_dealNum
        ,sum(case when dt >= '{day60_str}' then dealSum end) as day60_dealSum
        ,sum(case when dt >= '{day60_str}' then dealStockNum end) as day60_dealStockNum
        ,sum(case when dt >= '{day60_str}' then dealStockSum end) as day60_dealStockSum
        ,sum(case when dt >= '{day60_str}' then dealFundNum end) as day60_dealFundNum
        ,sum(case when dt >= '{day60_str}' then dealFundSum end) as day60_dealFundSum
        ,sum(case when dt >= '{day60_str}' then closeNum end) as day60_closeNum
        ,count(case when dt >= '{day60_str}' then dealNum end) as day60_deal_active_num
        ,sum(case when dt >= '{day90_str}' then dealNum end) as day90_dealNum
        ,sum(case when dt >= '{day90_str}' then dealSum end) as day90_dealSum
        ,sum(case when dt >= '{day90_str}' then dealStockNum end) as day90_dealStockNum
        ,sum(case when dt >= '{day90_str}' then dealStockSum end) as day90_dealStockSum
        ,sum(case when dt >= '{day90_str}' then dealFundNum end) as day90_dealFundNum
        ,sum(case when dt >= '{day90_str}' then dealFundSum end) as day90_dealFundSum
        ,sum(case when dt >= '{day90_str}' then closeNum end) as day90_closeNum
        ,count(case when dt >= '{day90_str}' then dealNum end) as day90_deal_active_num
        ,sum(case when dt >= '{day365_str}' then dealNum end) as day365_dealNum
        ,sum(case when dt >= '{day365_str}' then dealSum end) as day365_dealSum
        ,sum(case when dt >= '{day365_str}' then dealStockNum end) as day365_dealStockNum
        ,sum(case when dt >= '{day365_str}' then dealStockSum end) as day365_dealStockSum
        ,sum(case when dt >= '{day365_str}' then dealFundNum end) as day365_dealFundNum
        ,sum(case when dt >= '{day365_str}' then dealFundSum end) as day365_dealFundSum
        ,sum(case when dt >= '{day365_str}' then closeNum end) as day365_closeNum
        ,count(case when dt >= '{day365_str}' then dealNum end) as day365_deal_active_num
        from {db_name+'.'}deal
        where dt between '{day365_str}' and '{cur_str}' and {label_column} > 0
        group by customerId
    ),
    
    tmp_app_stat as (
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
        ,min(datediff(from_unixtime(UNIX_TIMESTAMP('{cur_str}', 'yyyymmdd')), from_unixtime(UNIX_TIMESTAMP(dt, 'yyyymmdd')))) as app_active_duration_days
        from {db_name+'.'}app
        where dt between '{day365_str}' and '{cur_str}' and {label_column} > 0 and isLogin=1
        group by customerId
    )
    
    select t1.customerId
            ,t1.label
            ,t2.sex,
            ,t2.age
            ,t2.riskScore1
            ,t2.riskScore2
            ,datediff(from_unixtime(unix_timestamp('{cur_str}', 'yyyymmdd')), from_unixtime(unix_timestamp(t2.dateOpen, 'yyyymmdd'))) as open_duration_days
            ,t2.customerType
            ,coalesce(t3.day3_dealNum, 0)
            ,coalesce(t3.day3_dealSum, 0)
            ,coalesce(t3.day3_dealStockNum, 0)
            ,coalesce(t3.day3_dealStockSum, 0)
            ,coalesce(t3.day3_dealFundNum, 0)
            ,coalesce(t3.day3_dealFundSum, 0)
            ,coalesce(t3.day3_closeNum, 0)
            ,coalesce(t3.day3_deal_active_num, 0)
            ,coalesce(t3.day7_dealNum, 0)
            ,coalesce(t3.day7_dealSum, 0)
            ,coalesce(t3.day7_dealStockNum, 0)
            ,coalesce(t3.day7_dealStockSum, 0)
            ,coalesce(t3.day7_dealFundNum, 0)
            ,coalesce(t3.day7_dealFundSum, 0)
            ,coalesce(t3.day7_closeNum, 0)
            ,coalesce(t3.day7_deal_active_num, 0)
            ,coalesce(t3.day30_dealNum, 0)
            ,coalesce(t3.day30_dealSum, 0)
            ,coalesce(t3.day30_dealStockNum, 0)
            ,coalesce(t3.day30_dealStockSum, 0)
            ,coalesce(t3.day30_dealFundNum, 0)
            ,coalesce(t3.day30_dealFundSum, 0)
            ,coalesce(t3.day30_closeNum, 0)
            ,coalesce(t3.day30_deal_active_num, 0)
            ,coalesce(t3.day60_dealNum, 0)
            ,coalesce(t3.day60_dealSum, 0)
            ,coalesce(t3.day60_dealStockNum, 0)
            ,coalesce(t3.day60_dealStockSum, 0)
            ,coalesce(t3.day60_dealFundNum, 0)
            ,coalesce(t3.day60_dealFundSum, 0)
            ,coalesce(t3.day60_closeNum, 0)
            ,coalesce(t3.day60_deal_active_num, 0)
            ,coalesce(t3.day90_dealNum, 0)
            ,coalesce(t3.day90_dealSum, 0)
            ,coalesce(t3.day90_dealStockNum, 0)
            ,coalesce(t3.day90_dealStockSum, 0)
            ,coalesce(t3.day90_dealFundNum, 0)
            ,coalesce(t3.day90_dealFundSum, 0)
            ,coalesce(t3.day90_closeNum, 0)
            ,coalesce(t3.day90_deal_active_num, 0)
            ,coalesce(t3.day365_dealNum, 0)
            ,coalesce(t3.day365_dealSum, 0)
            ,coalesce(t3.day365_dealStockNum, 0)
            ,coalesce(t3.day365_dealStockSum, 0)
            ,coalesce(t3.day365_dealFundNum, 0)
            ,coalesce(t3.day365_dealFundSum, 0)
            ,coalesce(t3.day365_closeNum, 0)
            ,coalesce(t3.day365_deal_active_num, 0)
            ,coalesce(t4.day3_login_num, 0)
            ,coalesce(t4.day3_news_click_num, 0)
            ,coalesce(t4.day7_login_num, 0)
            ,coalesce(t4.day7_news_click_num, 0)
            ,coalesce(t4.day30_login_num, 0)
            ,coalesce(t4.day30_news_click_num, 0)
            ,coalesce(t4.day60_login_num, 0)
            ,coalesce(t4.day60_news_click_num, 0)
            ,coalesce(t4.day90_login_num, 0)
            ,coalesce(t4.day90_news_click_num, 0)
            ,coalesce(t4.day365_login_num, 0)
            ,coalesce(t4.day365_news_click_num, 0)
    from tmp_label t1
    left join {db_name+'.'}customer t2 on t1.customerId = t2.customerId
    left join tmp_deal_stat t3 on t1.customerId = t3.customerId
    left join tmp_app_stat t4 on t1.customerId = t4.customerId
    """
    with open(output_file, 'w') as f:
        f.write(sql)
    df = df_hive_helper.query_to_df(sql)
    dir_name = os.path.dirname(output_file)
    logging.info(f"dir_name: {dir_name}")
    if dir_name and not os.path.exists(dir_name):
        logging.info(f"mkdir -p {dir_name}")
        os.system(f"mkdir -p {dir_name}")

    logging.info(f"write data to {output_file}")
    df.to_csv(output_file, index=False)


def main():
    parse = argparse.ArgumentParser()
    parse.add_argument('--cur_str', type=str, required=True)
    parse.add_argument('--output_file', type=str, required=True)
    parse.add_argument('--train_duration', type=int, default=90)
    parse.add_argument('--predict_duration', type=int, default=30)
    parse.add_argument('--label_column', type=str, default='deal_num')
    parse.add_argument('--db_name', type=str, default='default')
    parse.add_argument('--info_log_file', type=str, required=True)
    parse.add_argument('--error_log_file', type=str, required=True)

    args = parse.parse_args()
    cur_str = args.cur_str
    output_file = args.output_file
    train_duration = args.train_duration
    predict_duration = args.predict_duration
    label_column = args.label_column
    db_name = args.db_name
    info_log_file = args.info_log_file
    error_log_file = args.error_log_file

    setup_logging(info_log_file, error_log_file)
    get_examples(cur_str, output_file, train_duration, predict_duration, label_column, db_name)


if __name__ == '__main__':
    main()
