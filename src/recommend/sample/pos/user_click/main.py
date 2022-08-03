import sys

from digitforce.aip.common.hive_helper import df_hive_helper


def main():
    start_datetime = sys.argv[1]
    end_datetime = sys.argv[2]
    result_table = sys.argv[3]
    _sql = f'''
    WITH t_user_actions AS (
        SELECT user_id, sku 
        FROM labelx.push_traffic_behavior
        WHERE event_time > '{start_datetime}' AND envet_time <= '{end_datetime}' AND event_code = "CLICK" 
    )
    
    SELECT 
        user_id, 
        sku, 
        COUNT(*) AS click_cnt  
    FROM t_user_actions 
    GROUP BY user_id 
    '''
    df_hive_helper.query_to_table(_sql, result_table)


if __name__ == '__main__':
    main()
