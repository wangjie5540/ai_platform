import digitforce.aip.common.utils.config_helper as config_helper


def write_score(df, table_name):
    if '.' in table_name:
        table_name = table_name.split('.')[1]
    config = config_helper.get_module_config('starrocks')
    df.write.format("com.digitforce.bdp.StarrocksSource") \
        .option("database-name", config['db']) \
        .option("table-name", table_name) \
        .option("jdbc-url", config['jdbc_url']) \
        .option("load-url", config['load_url']) \
        .option("username", config['user']) \
        .option("password", config['password']) \
        .save()
