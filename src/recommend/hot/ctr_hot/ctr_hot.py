from digitforce.aip.common.hive_helper import dg_hive_helper


def calculate_hot_item(table_name, output_file):
    # calculate avg ctr
    df = dg_hive_helper.query_to_df(table_name, f"SELECT SUM(click_cnt) / (1 + COUNT(*)) AS ctr FROM {table_name}")
    avg_ctr = df["ctr"][0]
    ctr_sql = f'''
    SELECT item_id, SUM(click_cnt) + 100 / (SUM(show_cnt) + {100 / avg_ctr}) AS score 
    FROM {table_name} 
    ORDER BY score DESC 
    '''
    df = dg_hive_helper.query_to_df(table_name, ctr_sql)
    df.to_csv(output_file, index=False, header=None)
