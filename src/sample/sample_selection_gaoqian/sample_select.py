import datetime
import digitforce.aip.common.utils.spark_helper as spark_helper

DATE_FORMAT = "%Y-%m-%d"

def sample_create(event_table_name, event_columns, item_table_name, item_columns, event_code_list, category_a, train_period, predict_period):
    spark_client = spark_helper.SparkClient()

    today = datetime.datetime.today().date()
    one_year_ago = (today - datetime.timedelta(365)).strftime(DATE_FORMAT)
    user_id = event_columns[0]
    item_id = event_columns[2]
    trade_date = event_columns[-1]
    trade_type = event_columns[1]
    item_id_item = item_columns[0]
    fund_type = item_columns[1]

    event_data = spark_client.get_starrocks_table_df(event_table_name)
    event_data = event_data.select(event_columns)\
        .filter((event_data[trade_date] >= one_year_ago) & (event_data[trade_date] < today))

    item_data = spark_client.get_starrocks_table_df(item_table_name)
    item_data = item_data.select(item_columns).distinct()

    join_data = event_data.join(item_data.select([item_id_item, fund_type]), event_data[item_id] == item_data[item_id_item])

    columns = [user_id, trade_type, trade_date, fund_type]
    data = join_data.select(columns)

    max_trade_date = data.filter((data[trade_type] == event_code_list[0]) & (data[fund_type] == category_a))\
        .select(trade_date).rdd.max()[0]
    interval = max(train_period, predict_period)
    date_mid_str = (max_trade_date - datetime.timedelta(days=interval)).strftime(DATE_FORMAT)
    date_from_str = (datetime.datetime.strptime(date_mid_str, DATE_FORMAT) - datetime.timedelta(days=train_period)).strftime(DATE_FORMAT)
    date_end_str = (datetime.datetime.strptime(date_mid_str, DATE_FORMAT) + datetime.timedelta(days=predict_period)).strftime(DATE_FORMAT)

    sample_a = data.filter((data[trade_date] >= date_from_str) & (data[trade_date] < date_mid_str))
    sample_b = data.filter((data[trade_date] >= date_mid_str) & (data[trade_date] < date_end_str))

    if len(event_code_list) == 1:
        sample_a_rdd = sample_a.filter(~((sample_a[trade_type] == event_code_list[0]) & (sample_a[fund_type] == category_a))).rdd\
            .map(lambda x: (x[0], 0))\
            .reduceByKey(lambda x, y: x + y)
        sample_b_rdd = sample_b.filter(((sample_a[trade_type] == event_code_list[0]) & (sample_a[fund_type] == category_a))).rdd\
            .map(lambda x: (x[0], 0))\
            .reduceByKey(lambda x, y: x + y)
        sample_columns = ['user_id', 'label']
        sample_df = sample_a_rdd.map(lambda x: (x[0], 0))\
            .union(sample_b_rdd.map(lambda x: (x[0], 1)))\
            .reduceByKey(lambda x, y: x+y)\
            .toDF(sample_columns)
    else:
        sample_a_rdd = sample_a.filter(~(((sample_a[trade_type] == event_code_list[0]) | (sample_a[trade_type] == event_code_list[1])) & (sample_a[fund_type] == category_a))).rdd\
            .map(lambda x: (x[0], 0))\
            .reduceByKey(lambda x, y: x + y)
        sample_b_rdd1 = sample_b.filter(((sample_b[trade_type] == event_code_list[0]) & (sample_b[fund_type] == category_a))).rdd \
            .map(lambda x: (x[0], 0))\
            .reduceByKey(lambda x, y: x + y)
        sample_b_rdd2 = sample_b.filter(((sample_b[trade_type] == event_code_list[1]) & (sample_b[fund_type] == category_a))).rdd\
            .map(lambda x: (x[0], 0))\
            .reduceByKey(lambda x, y: x + y)
        sample_columns = ['user_id', 'label1', 'label2']
        sample_df = sample_a_rdd.map(lambda x: (x[0], (0, 0)))\
            .union(sample_b_rdd1.map(lambda x: (x[0], (1, 0))))\
            .union(sample_b_rdd2.map(lambda x: (x[0], (0, 1))))\
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
            .map(lambda x: (x[0], x[1][0], x[1][1]))\
            .toDF(sample_columns)

    # todo: dynamic change table name
    sample_table_name = 'algorithm.tmp_aip_sample_gaoqian'
    print(sample_df.show(20))
    sample_df.write.format("hive").mode("overwrite").saveAsTable(sample_table_name)
    return sample_table_name, sample_columns


