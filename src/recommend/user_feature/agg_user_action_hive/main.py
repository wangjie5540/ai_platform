from digitforce.aip.common.hive_helper import df_hive_helper


def main():
    start_datetime = sys.argv[1]
    end_datetime = sys.argv[2]
    result_table = sys.argv[3]
    _sql = f'''
    WITH t_user_actions AS (
        SELECT event_time, event_code, user_id, sku 
        FROM labelx.push_traffic_behavior
        WHERE event_time > '{start_datetime}' AND envet_time <= '{end_datetime}' 
    )
    
    SELECT 
        user_id, 
        COLLECT_LIST(
            named_struct(
                'event_time', event_time,
                'sku', sku,
                'event_code', event_code)) AS user_action_array  
    FROM t_user_actions
    '''
    df_hive_helper.query_to_table(_sql, result_table)


if __name__ == '__main__':
    from digitforce.aip.common.logging_config import setup_console_log
    import sys

    setup_console_log()
    main()
