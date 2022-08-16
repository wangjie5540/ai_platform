from digitforce.aip.common.file_helper import create_dir
from digitforce.aip.common.hive_helper import df_hive_helper


def calculate_hot_item(table_name, output_file):
    click_sql = f'''
    SELECT item_id, SUM(click_cnt) AS score
    FROM {table_name} 
    GROUP BY item_id 
    ORDER BY score DESC 
    '''
    df = df_hive_helper.query_to_df(click_sql)
    create_dir(output_file)
    df.to_csv(output_file, index=False, header=None)


