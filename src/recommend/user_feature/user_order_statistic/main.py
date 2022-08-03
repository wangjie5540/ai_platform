import sys

from digitforce.aip.common.hive_helper import df_hive_helper


def main():
    start_datetime = sys.argv[1]
    end_datetime = sys.argv[2]
    result_table = sys.argv[3]
    _sql = f'''
WITH t_user_order AS (
    SELECT order_time AS event_time, user_id, sku  
    FROM labelx.push_order_behavior
    WHERE order_time >= '{start_datetime}' AND order_time <= '{end_datetime}' 
),
t_user_product_order_cnt AS (
    SELECT 
        user_id, 
        COUNT(*) AS order_cnt
    FROM t_user_order 
    GROUP BY user_id, sku_id  
)
    SELECT 
        user_id, 
        COLLECT_LIST(named_struck('sku':sku, 'order_cnt',order_cnt)) AS user_sku_order_cnt 
    FROM t_user_order 
    GROUP BY user_id  
    '''
    df_hive_helper.query_to_table(_sql, result_table)


if __name__ == '__main__':
    from digitforce.aip.common.logging_config import setup_console_log
    import sys

    setup_console_log()
    main()
