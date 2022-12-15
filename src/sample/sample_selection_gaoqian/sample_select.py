import math
import builtins
import random
import datetime
import digitforce.aip.common.utils.spark_helper as spark_helper

DATE_FORMAT = "%Y-%m-%d"

def sample_create(data_table_name, columns, event_code_list, category_a, category_b, train_period, predict_period):
    spark_client = spark_helper.SparkClient()
    # data = [user_id, item_id, trade_type, fund_type, dt]
    user_id = columns[0]
    item_id = columns[1]
    trade_type = columns[2]
    fund_type = columns[3]
    trade_date = columns[4]

    columns_str = ",".join(columns)
    data = spark_client.get_session().sql(f'select {columns_str} from {data_table_name}')
    if len(event_code_list) == 1:
        data = data.filter(data[trade_type] == event_code_list[0])
    else:
        data = data.filter((data[trade_type] == event_code_list[0]) | (data[trade_type] == event_code_list[1]))

    max_trade_date = data.select(trade_date).rdd.max()[0]
    interval = max(train_period, predict_period)
    date_mid_str = (max_trade_date - datetime.timedelta(days=interval)).strftime(DATE_FORMAT)
    date_from_str = (datetime.datetime.strptime(date_mid_str, DATE_FORMAT) - datetime.timedelta(days=train_period)).strftime(DATE_FORMAT)
    date_end_str = (datetime.datetime.strptime(date_mid_str, DATE_FORMAT) + datetime.timedelta(days=predict_period)).strftime(DATE_FORMAT)

    sample_a = data.filter((data[trade_date] >= date_from_str) & (data[trade_date] < date_mid_str) & (data[fund_type] == category_a))
    sample_b = data.filter((data[trade_date] >= date_mid_str) & (data[trade_date] < date_end_str) & (data[fund_type] == category_b))

    if len(event_code_list) == 1:
        sample_a_rdd = sample_a.rdd.map(lambda x: (x[0], 0))\
            .reduceByKey(lambda x, y: x + y)
        sample_b_rdd = sample_b.rdd.map(lambda x: (x[0], 0))\
            .reduceByKey(lambda x, y: x + y)
        sample_columns = ['user_id', 'label']
        sample_df = sample_a_rdd.map(lambda x: (x[0], 0))\
            .union(sample_b_rdd.map(lambda x: (x[0], 1)))\
            .reduceByKey(lambda x, y: x+y)\
            .toDF(sample_columns)
    else:
        sample_a_rdd = sample_a.rdd.map(lambda x: (x[0], 0))\
            .reduceByKey(lambda x, y: x + y)
        sample_b_rdd1 = sample_b.filter(sample_b[trade_type] == event_code_list[0]).rdd \
            .map(lambda x: (x[0], 0))\
            .reduceByKey(lambda x, y: x + y)
        sample_b_rdd2 = sample_b.filter(sample_b[trade_type] == event_code_list[1]).rdd\
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
    sample_df.write.format("hive").mode("overwrite").saveAsTable(sample_table_name)
    return sample_table_name, sample_columns


