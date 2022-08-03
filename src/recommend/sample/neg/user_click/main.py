import sys

from digitforce.aip.common.hive_helper import df_hive_helper


def main():
    start_datetime = sys.argv[1]
    end_datetime = sys.argv[2]
    result_table = sys.argv[3]
    _sql = f'''
    WITH t_user_show AS (
        SELECT user_id, sku, COUNT(*) AS show_cnt 
        FROM labelx.push_traffic_behavior
        WHERE event_time > '{start_datetime}' AND envet_time <= '{end_datetime}' AND event_code = "EXPOSURE" 
        GROUP BY user_id, sku
    ),
    t_user_click AS (
        SELECT user_id, sku, COUNT(*) AS click_cnt 
        FROM labelx.push_traffic_behavior 
        WHERE event_time > '{start_datetime}' AND envet_time <= '{end_datetime}' AND event_code = "EXPOSURE" 
        GROUP BY user_id, sku
    ),
    t_show_and_click AS (
        SELECT user_id, sku_id, show_cnt, click_cnt   
        FROM t_user_show 
        LEFT JOIN t_user_click ON t_user_click.user_id = t_user_click.user_id AND t_user_click.sku = t_user_show.sku 
    )
    
    SELECT 
        user_id, 
        sku, 
        0 AS click_cnt 
    FROM t_show_and_click 
    WHERE show_cnt > 0 AND click_cnt IS NULL  
    '''
    df_hive_helper.query_to_table(_sql, result_table)


if __name__ == '__main__':
    main()
