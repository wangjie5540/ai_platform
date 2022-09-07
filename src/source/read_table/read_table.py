# coding: utf-8
from digitforce.aip.common.utils.spark_helper import SparkClient
import digitforce.aip.common.utils.id_helper as id_helper


def read_starrocks_table_df(source_desc):
    spark_client = SparkClient()
    # 使用spark的view进行表名替换
    table_mapping = dict()
    table_mapping[source_desc['from']] = create_tmp_view(spark_client, source_desc['from'])
    for join in source_desc.get('joins', list()):
        table_mapping[join['join']] = create_tmp_view(spark_client, join['join'])
    sql = source_desc_to_sql(source_desc, table_mapping)
    print("read from starrocks. sql=", sql)
    return f"read_table_{id_helper.gen_uniq_id()}", spark_client.get_session().sql(sql)


def read_table_to_hive(source_desc):
    tmp_name, df = read_starrocks_table_df(source_desc)
    columns_list = [select['as'] for select in source_desc['selects']]
    table_name = f'aip.{tmp_name}'
    df.write.format("Hive").mode('overwrite').saveAsTable(table_name)
    return [
        {
            "type": "hive_table",
            "table_name": table_name,
            "column_list": ','.join(columns_list),
        },
    ]


def source_desc_to_sql(source_desc, table_mapping):
    selects = source_desc['selects']
    from_table = table_mapping[source_desc['from']]
    joins = source_desc.get('joins', list())
    where = source_desc['where']
    dt = source_desc.get('dt', None)

    column_list = list()
    for select in selects:
        column_list.append(f"{table_mapping[select['dbtable']]}.{select['column']} as {select['as']}")

    join_list = list()
    for join in joins:
        join_list.append(f"left join {join['join']} on {join['on']}")

    if dt is not None:
        dt_clause = f"dt between '{dt['start']}' and '{dt['end']}"
    else:
        dt_clause = '1=1'

    return rf"""
        select {','.join(column_list)} from {from_table} {' '.join(join_list)} where 1=1 and {where} and {dt_clause}
        """


def create_tmp_view(spark_client, dbtable):
    view_name = f"{dbtable.split('.')[-1]}_{id_helper.gen_uniq_id()}"
    df = spark_client.get_starrocks_table_df(table_name=dbtable)
    df.createTempView(view_name)
    return view_name


# aaa = {
#     "selects": [
#         {
#             "dbtable": "aip.item",
#             "column": "item_id",
#             "as": "item_id"
#         },
#         {
#             "dbtable": "aip.item",
#             "column": "item_type",
#             "as": "item_type"
#         },
#     ],
#     "from": "aip.item",
#     "where": "1=1",
# }

# print(read_table_to_hive(aaa))
