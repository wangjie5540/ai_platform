from digitforce.aip.common.hive_helper import dg_hive_helper


def generate_train_data(input_table, output_file, profile_col_name):
    sql = f'''
    SELECT 
        user_id, 
        item_id, 
        {profile_col_name} AS profile_id, 
        click_cnt, 
        save_cnt, 
        order_cnt,
        event_timestamp  
    FROM {input_table}
    WHERE click_cnt > 0 OR save_cnt > 0 OR order_cnt > 0 
    ORDER BY user_id, event_timestamp DESC
'''
    df = dg_hive_helper.query_to_df(sql)
    df.to_csv(output_file, index=False, header=None)
