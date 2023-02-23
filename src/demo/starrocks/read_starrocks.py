import digitforce.aip.common.utils.spark_helper as spark_helper


def do_read(table_name, limit):
    spark_client = spark_helper.SparkClient.get()
    spark_client.get_starrocks_table_df(table_name).show(limit)
