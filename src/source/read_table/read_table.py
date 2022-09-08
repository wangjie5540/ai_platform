# coding: utf-8
from digitforce.aip.common.utils.spark_helper import SparkClient
import digitforce.aip.common.utils.id_helper as id_helper


def read_starrocks_table_df(source_desc):
    spark_client = SparkClient()
    # 使用spark的view进行表名替换
    source_desc['from'] = create_tmp_view(spark_client, source_desc['from'])
    for join in source_desc.get('joins', list()):
        join['join'] = create_tmp_view(spark_client, join['join'])
    sql = source_desc_to_sql(source_desc)
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


def source_desc_to_sql(source_desc):
    selects = source_desc['selects']
    from_table = source_desc['from']
    joins = source_desc.get('joins', list())
    where = source_desc.get('where', '1=1')
    dt = source_desc.get('dt', None)

    column_list = list()
    sub_column_list = list()
    for select in selects:
        column_desc = f"{select['table']}.{select['column']} as {select['as']}"
        column_list.append(column_desc)
        if select['table'] == from_table:
            sub_column_list.append(column_desc)

    join_list = list()
    for join in joins:
        join_list.append(f"left join {join['join']} on {join['on']}")

    if dt is not None:
        dt_clause = f"dt between '{dt['start']}' and '{dt['end']}'"
    else:
        dt_clause = '1=1'

    return rf"""
        select {','.join(column_list)} from (select {','.join(sub_column_list)} from {from_table} where {dt_clause}) as {from_table} {' '.join(join_list)} where 1=1 and {where}
        """


def create_tmp_view(spark_client, table):
    view_name = f"{table.split('.')[-1]}"
    df = spark_client.get_starrocks_table_df(table_name=f'aip.{table}')
    df.createTempView(view_name)
    return view_name


# aaa = {
#     "selects": [
#         {
#             "table": "item",
#             "column": "item_id",
#             "as": "item_id"
#         },
#         {
#             "table": "item",
#             "column": "item_type",
#             "as": "item_type"
#         },
#         {
#             "table": "item1",
#             "column": "title",
#             "as": "title"
#         }
#     ],
#     "from": "item",
#     # "dt": {
#     #     "start": "2022-01-01",
#     #     "end": "2022-08-08",
#     #     "offset": -9
#     # },
#     "joins": [
#         {
#             "join": "item1",
#             "on": "item.item_id = item1.item_id"
#         }
#     ],
#     "where": "item.item_id != 'aaa'"
# }
#
# print(read_table_to_hive(aaa))
