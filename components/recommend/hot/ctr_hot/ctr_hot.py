import sys


def hive_sql_executor(table_name, sql):
    # todo executor hive sql return DataFrame
    return None


def df_to_hive(df, table_name, dataset='ai'):
    # todo save DataFrame to hive
    return table_name


def calculate_hot_item(table_name, output_file):
    # calculate avg ctr
    df = hive_sql_executor(table_name, f"SELECT SUM(click_cnt) / (1 + COUNT(*)) AS ctr FROM {table_name}")
    avg_ctr = df["ctr"][0]
    # todo
    avg_ctr = 0.01
    # calculate ctr
    ctr_sql = f'''
    SELECT item_id, SUM(click_cnt) + 100 / (SUM(show_cnt) + {100 / avg_ctr}) AS ctr 
    FROM {table_name} 
    ORDER BY ctr DESC 
    '''
    df = hive_sql_executor(table_name, ctr_sql)
    return df


def main():
    from common.logging_config import setup_console_log
    setup_console_log()
    input_file, output_file = sys.argv[1], sys.argv[2]
    calculate_hot_item(input_file, output_file)


if __name__ == '__main__':
    # main()
    from pyhive import hive
    import pandas as pd
    conn = hive.Connection(host="bigdata-server-09", port=10000)
    df = pd.read_sql("select * from algorithm.sku_profile limit 100", conn)
    print(df)
