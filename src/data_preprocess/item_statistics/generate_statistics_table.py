from digitforce.aip.common.hive_helper import df_hive_helper
from datetime import timedelta, datetime
import logging


def generate_item_statistics_table(statistics_table, table_name='labelx.push_traffic_behavior',
                                   event_code_column_name='event_code', duration=None, partition_name='dt',
                                   date_str=None, item='sku'):
    # 终止时间
    dt = datetime.strptime(date_str, '%Y-%m-%d') if date_str else datetime.now()
    end_dt = dt.strftime('%Y-%m-%d')
    date_dict = {}
    d1 = (dt - timedelta(days=1)).strftime('%Y-%m-%d')
    date_dict[d1] = 'd1'
    d7 = (dt - timedelta(days=7)).strftime('%Y-%m-%d')
    date_dict[d7] = 'd7'
    d15 = (dt - timedelta(days=15)).strftime('%Y-%m-%d')
    date_dict[d15] = 'd15'
    d30 = (dt - timedelta(days=30)).strftime('%Y-%m-%d')
    date_dict[d30] = 'd30'
    d60 = (dt - timedelta(days=60)).strftime('%Y-%m-%d')
    date_dict[d60] = 'd60'
    d90 = (dt - timedelta(days=90)).strftime('%Y-%m-%d')
    date_dict[d90] = 'd90'
    d180 = (dt - timedelta(days=180)).strftime('%Y-%m-%d')
    date_dict[d180] = 'd180'

    dt_cate = [d1, d7, d15, d30, d60, d90, d180]
    if not duration:
        cond = f'where 1 = 1'
    else:
        delta = timedelta(days=duration)
        start_dt = (dt - delta).strftime('%Y-%m-%d')
        cond = f'where {partition_name} > "{start_dt}" and {partition_name} <= "{end_dt}'
    sql = f'select distinct {event_code_column_name} from {table_name} {cond}'
    logging.info(f'select event_code sql: {sql}')
    event_code_df = df_hive_helper.query_to_df(sql)

    event_codes = event_code_df[event_code_column_name].tolist()
    logging.info(f'event_codes: {event_codes}')
    sql_str = f'select {item}'
    for event_code in event_codes:
        for dc in dt_cate:
            sub_sql_str = f"\n, sum(case when {partition_name} > '{dc}' and {partition_name} <= '{end_dt}' then 1 " \
                          f"else 0 end) as {event_code + '_' + date_dict[dc]}"
            sql_str += sub_sql_str

    sql_statistics = f"{sql_str} \n,{end_dt} as dt from {table_name} {cond} group by {item}"
    logging.info(f'generate sql: {sql_statistics}')
    df_hive_helper.query_to_table(sql_statistics, statistics_table, delete_tb=True)

