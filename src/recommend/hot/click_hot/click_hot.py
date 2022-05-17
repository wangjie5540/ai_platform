from digitforce.aip.common.hive_helper import dg_hive_helper


def calculate_hot_item(table_name, output_file):
    click_sql = f'''
    SELECT item_id, SUM(click_cnt) AS score
    FROM {table_name} 
    ORDER BY score DESC 
    '''
    df = dg_hive_helper.query_to_df(table_name, click_sql)
    df.to_csv(output_file, index=False, header=None)


