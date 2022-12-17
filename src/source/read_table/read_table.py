# coding: utf-8
import digitforce.aip.common.utils.spark_helper as spark_helper
import digitforce.aip.common.utils.id_helper as id_helper


def read_table_to_hive(select_sql):
    """
    读取数据到hive表
    :param select_sql:
    :return:
    """
    # 使用spark的view进行表名替换
    spark_client = spark_helper.SparkClient()
    df = spark_client.get_starrocks_table_df(table_name=f"""({select_sql}) as t""")
    view_name = f'read_table_{id_helper.gen_uniq_id()}'
    df.createTempView(view_name)
    table_name = f'aip.{view_name}'
    df.write.format("Hive").mode('overwrite').saveAsTable(table_name)
    return table_name
