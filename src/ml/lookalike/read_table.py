# coding: utf-8
from digitforce.aip.common.utils.spark_helper import SparkClient
import digitforce.aip.common.utils.id_helper as id_helper


def read_starrocks_table_df(table, column_list, filter_condition, start_dt, end_dt):
    spark_client = SparkClient()
    df = spark_client.get_starrocks_table_df(table_name=table)
    tmp_name = f"{table.split('.')[-1]}_{id_helper.gen_uniq_id()}"
    df.createTempView(tmp_name)
    condition_list = list()
    condition_list.append("1=1")
    if filter_condition is not None and len(filter_condition) != 0:
        condition_list.append(filter_condition)
    if start_dt is not None and end_dt is not None:
        condition_list.append(f"dt between {start_dt} and {end_dt}")
    condition_str = ' and '.join(condition_list)
    sql = rf"""
    select {','.join(column_list)} from {tmp_name} where {condition_str} 
    """
    return tmp_name, spark_client.get_session().sql(sql)


def read_table_to_hive(table, column_list, filter_condition, start_dt, end_dt):
    tmp_name, df = read_starrocks_table_df(table, column_list, filter_condition, start_dt, end_dt)
    df.write.format("Hive").mode('overwrite').saveAsTable(f"aip.{tmp_name}")
    return {
        "output_table": f"aip.{tmp_name}",
        "column_list": column_list,
    }
